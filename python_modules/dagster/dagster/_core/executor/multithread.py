import os
import sys
import threading
import time
from collections.abc import Iterator, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import ExitStack
from queue import Queue
from typing import Any, Optional

from dagster import _check as check
from dagster._core.definitions.metadata import MetadataValue
from dagster._core.errors import DagsterExecutionInterruptedError, DagsterSubprocessError
from dagster._core.events import DagsterEvent, EngineEventData
from dagster._core.execution.api import create_execution_plan, execute_plan_iterator
from dagster._core.execution.context.system import IStepContext, PlanOrchestrationContext
from dagster._core.execution.plan.active import ActiveExecution
from dagster._core.execution.plan.instance_concurrency_context import InstanceConcurrencyContext
from dagster._core.execution.plan.plan import ExecutionPlan
from dagster._core.execution.retries import RetryMode
from dagster._core.executor.base import Executor
from dagster._core.instance import DagsterInstance
from dagster._utils.error import SerializableErrorInfo, serializable_error_info_from_exc_info
from dagster._utils.timing import format_duration, time_execution_scope


class MultithreadExecutor(Executor):
    """Executor that executes steps in parallel using threads.

    The multithread executor uses concurrent.futures.ThreadPoolExecutor to execute
    steps in parallel in separate threads.
    """

    def __init__(
        self,
        retries: RetryMode,
        max_concurrent: Optional[int] = None,
        tag_concurrency_limits: Optional[list[dict[str, Any]]] = None,
    ):
        self._retries = check.inst_param(retries, "retries", RetryMode)

        if not max_concurrent:
            env_var_default = os.getenv("DAGSTER_MULTITHREAD_EXECUTOR_MAX_CONCURRENT")
            max_concurrent = int(env_var_default) if env_var_default else os.cpu_count()

        self._max_concurrent = check.int_param(max_concurrent, "max_concurrent")
        self._tag_concurrency_limits = check.opt_list_param(
            tag_concurrency_limits, "tag_concurrency_limits"
        )

    @property
    def retries(self) -> RetryMode:
        return self._retries

    def safe_execute_step_with_event_queue(
        self,
        event_queue: Queue[DagsterEvent],
        step_context: IStepContext,
        step_key: str,
        term_event: threading.Event,
        active_execution: ActiveExecution,
        job_execution_plan: ExecutionPlan,
        instance: DagsterInstance,
    ) -> None:
        """Execute a step with interrupts captured."""
        try:
            for step_event in self.execute_step_in_thread(
                step_context=step_context,
                step_key=step_key,
                term_event=term_event,
                active_execution=active_execution,
                job_execution_plan=job_execution_plan,
                instance=instance,
            ):
                if term_event.is_set():
                    return
                event_queue.put(step_event)
        except (
            Exception,
            KeyboardInterrupt,
            DagsterExecutionInterruptedError,
        ) as err:
            term_event.set()
            raise err

    def execute_step_in_thread(
        self,
        step_context: IStepContext,
        step_key: str,
        term_event: threading.Event,
        active_execution: ActiveExecution,
        job_execution_plan: ExecutionPlan,
        instance: DagsterInstance,
    ) -> Iterator[DagsterEvent]:
        """Execute a single step in a thread and return results or error."""
        try:
            step_execution_plan = create_execution_plan(
                job=step_context.job,
                run_config=step_context.run_config,
                step_keys_to_execute=[step_key],
                known_state=active_execution.get_known_state(),
                repository_load_data=job_execution_plan.repository_load_data,
            )

            iterator = execute_plan_iterator(
                step_execution_plan,
                step_context.job,
                step_context.dagster_run,
                run_config=step_context.run_config,
                retry_mode=self.retries.for_inner_plan(),
                instance=instance,
            )

            yield from iterator

        finally:
            # set events to stop the termination thread on exit
            term_event.set()

    def execute(
        self, plan_context: PlanOrchestrationContext, execution_plan: ExecutionPlan
    ) -> Iterator[DagsterEvent]:
        """Execute the plan using a ThreadPoolExecutor to run steps in parallel threads.

        Args:
            plan_context: The context for orchestrating plan execution
            execution_plan: The execution plan to execute

        Yields:
            DagsterEvent: Events from the execution
        """
        check.inst_param(plan_context, "plan_context", PlanOrchestrationContext)
        check.inst_param(execution_plan, "execution_plan", ExecutionPlan)

        step_keys_to_execute = execution_plan.step_keys_to_execute

        # Announce start of execution
        yield DagsterEvent.engine_event(
            plan_context,
            f"Executing steps using multithread executor (pid: {os.getpid()}, thread: {threading.get_ident()})",
            event_specific_data=EngineEventData.in_process(
                os.getpid(), step_keys_to_execute=step_keys_to_execute
            ),
        )

        # Dict to track futures and their associated step keys
        futures: Mapping[str, Future] = {}
        # Dict to track errors by step key
        errors: Mapping[str, SerializableErrorInfo] = {}
        # Flag to track if execution is being interrupted
        stopping = False
        # Termination event to signal threads to stop
        term_events: Mapping[str, threading.Event] = {}
        # Create event queue for child process communication
        event_queue = Queue()

        # Use ExitStack to manage multiple context managers
        with ExitStack() as stack:
            timer_result = stack.enter_context(time_execution_scope())

            instance_concurrency_context = stack.enter_context(
                InstanceConcurrencyContext(plan_context.instance, plan_context.dagster_run)
            )

            # Set up active execution to track dependencies and ready steps
            active_execution = stack.enter_context(
                ActiveExecution(
                    execution_plan,
                    retry_mode=self.retries,
                    max_concurrent=self._max_concurrent,
                    tag_concurrency_limits=self._tag_concurrency_limits,
                    instance_concurrency_context=instance_concurrency_context,
                )
            )

            # Create thread pool for executing steps
            thread_pool = stack.enter_context(ThreadPoolExecutor(max_workers=self._max_concurrent))

            try:
                # Main execution loop
                while (not stopping and not active_execution.is_complete) or futures:
                    # Check for interrupts
                    if active_execution.check_for_interrupts():
                        yield DagsterEvent.engine_event(
                            plan_context,
                            "Multithread executor: received termination signal",
                            EngineEventData.interrupted(list(futures.keys())),
                        )
                        stopping = True
                        thread_pool.shutdown(wait=False, cancel_futures=True)
                        for key, term_event in term_events.items():
                            if key in futures:
                                if futures[key].running():
                                    term_event.set()
                                del futures[key]
                        active_execution.mark_interrupted()

                    # Submit new steps if not stopping
                    if not stopping:
                        # Get steps ready for execution
                        steps_to_execute = active_execution.get_steps_to_execute(
                            limit=(self._max_concurrent - len(futures))
                        )

                        yield from active_execution.concurrency_event_iterator(plan_context)

                        # Submit new steps to thread pool
                        for step in steps_to_execute:
                            step_key = step.key
                            term_events[step_key] = threading.Event()
                            step_context = plan_context.for_step(step)

                            future = thread_pool.submit(
                                self.safe_execute_step_with_event_queue,
                                event_queue,
                                step_context,
                                step_key,
                                term_events[step_key],
                                active_execution,
                                execution_plan,
                                step_context.instance,
                            )
                            futures[step_key] = future

                            yield DagsterEvent.engine_event(
                                step_context,
                                "Starting step in thread",
                                EngineEventData.in_process(os.getpid()),
                            )

                    # Process completed futures (both success and error cases)
                    completed_steps = set()

                    # Check which futures are done
                    for step_key, future in futures.items():
                        if future.done():
                            completed_steps.add(step_key)

                    # Handle events
                    def __safe_queue_get_nowait() -> Optional[DagsterEvent]:
                        """Safely get events from the queue without blocking."""
                        try:
                            return event_queue.get_nowait()
                        except Exception:
                            return None

                    for event in iter(__safe_queue_get_nowait, None):
                        if event is not None:
                            yield event
                            active_execution.handle_event(event)

                    # Remove completed futures
                    for step_key in completed_steps:
                        active_execution.verify_complete(plan_context, step_key)
                        del futures[step_key]
                        del term_events[step_key]

                    # Process skipped and abandoned steps
                    yield from active_execution.plan_events_iterator(plan_context)

                    # Sleep briefly if no steps completed to avoid CPU spin
                    if not completed_steps and futures:
                        time.sleep(0.01)

            except Exception as exc:
                # Handle errors in the main execution loop
                if not stopping and futures:
                    error_info = serializable_error_info_from_exc_info(sys.exc_info())
                    yield DagsterEvent.engine_event(
                        plan_context,
                        "Unexpected exception in multithread executor",
                        EngineEventData(
                            metadata={
                                "steps_in_flight": MetadataValue.text(str(list(futures.keys())))
                            },
                            error=error_info,
                        ),
                    )
                for key, term_event in term_events.items():
                    if key in futures:
                        if futures[key].running():
                            term_event.set()
                        del futures[key]

                raise exc

            # Handle any errors from execution
            if errors and any(
                err.cls_name not in {"DagsterExecutionInterruptedError", "KeyboardInterrupt"}
                for err in errors.values()
            ):
                # If we have real errors (not just interrupts), raise them
                raise DagsterSubprocessError(
                    "During multithread execution errors occurred in steps:\n{error_list}".format(
                        error_list="\n".join(
                            [
                                f"In step {step_key}: {err.to_string()}"
                                for step_key, err in errors.items()
                            ]
                        )
                    ),
                    subprocess_error_infos=list(errors.values()),
                )

        # Announce end of execution
        if timer_result:
            yield DagsterEvent.engine_event(
                plan_context,
                f"Multithread executor: completed after {format_duration(timer_result.millis)} (pid: {os.getpid()}, thread: {threading.get_ident()})",
                event_specific_data=EngineEventData.in_process(os.getpid()),
            )
