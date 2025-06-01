import os
import sys
import threading
import time
from collections.abc import Iterator, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import ExitStack
from typing import TYPE_CHECKING, Any, Optional, cast

from dagster import _check as check
from dagster._core.definitions.metadata import MetadataValue
from dagster._core.definitions.reconstruct import ReconstructableJob
from dagster._core.definitions.repository_definition import RepositoryLoadData
from dagster._core.errors import DagsterSubprocessError
from dagster._core.events import DagsterEvent, EngineEventData
from dagster._core.execution.api import create_execution_plan, execute_plan_iterator
from dagster._core.execution.context.system import PlanOrchestrationContext
from dagster._core.execution.context_creation_job import create_context_free_log_manager
from dagster._core.execution.plan.active import ActiveExecution
from dagster._core.execution.plan.instance_concurrency_context import InstanceConcurrencyContext
from dagster._core.execution.plan.objects import StepFailureData
from dagster._core.execution.plan.plan import ExecutionPlan
from dagster._core.execution.plan.state import KnownExecutionState
from dagster._core.execution.retries import RetryMode
from dagster._core.executor.base import Executor
from dagster._core.instance import DagsterInstance
from dagster._utils import start_termination_thread
from dagster._utils.error import (
    ExceptionInfo,
    SerializableErrorInfo,
    serializable_error_info_from_exc_info,
)
from dagster._utils.timing import format_duration, time_execution_scope

if TYPE_CHECKING:
    from dagster._core.instance.ref import InstanceRef
    from dagster._core.storage.dagster_run import DagsterRun


class StepProcessExecutorChild:
    def __init__(
        self,
        run_config: Mapping[str, object],
        dagster_run: "DagsterRun",
        step_key: str,
        instance_ref: "InstanceRef",
        term_event: Any,
        recon_pipeline: ReconstructableJob,
        retry_mode: RetryMode,
        known_state: Optional[KnownExecutionState],
        repository_load_data: Optional[RepositoryLoadData],
    ):
        self.run_config = run_config
        self.dagster_run = dagster_run
        self.step_key = step_key
        self.instance_ref = instance_ref
        self.term_event = term_event
        self.recon_pipeline = recon_pipeline
        self.retry_mode = retry_mode
        self.known_state = known_state
        self.repository_load_data = repository_load_data

    def execute(self) -> Iterator[DagsterEvent]:
        recon_job = self.recon_pipeline
        with DagsterInstance.from_ref(self.instance_ref) as instance:
            done_event = threading.Event()
            start_termination_thread(self.term_event, done_event)
            try:
                log_manager = create_context_free_log_manager(instance, self.dagster_run)

                yield DagsterEvent.step_worker_started(
                    log_manager,
                    self.dagster_run.job_name,
                    message=f'Executing step "{self.step_key}" in subprocess.',
                    metadata={
                        "pid": MetadataValue.text(str(os.getpid())),
                    },
                    step_key=self.step_key,
                )
                execution_plan = create_execution_plan(
                    job=recon_job,
                    run_config=self.run_config,
                    step_keys_to_execute=[self.step_key],
                    known_state=self.known_state,
                    repository_load_data=self.repository_load_data,
                )
                yield from execute_plan_iterator(
                    execution_plan,
                    recon_job,
                    self.dagster_run,
                    run_config=self.run_config,
                    retry_mode=self.retry_mode.for_inner_plan(),
                    instance=instance,
                )
            finally:
                # set events to stop the termination thread on exit
                done_event.set()  # waiting on term_event so set done first
                self.term_event.set()


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

    def execute_step_in_thread(
        self, step_context, step_key, active_execution, job_execution_plan
    ) -> Iterator[DagsterEvent]:
        """Execute a single step in a thread and return results or error."""
        with DagsterInstance.from_ref(step_context.instance.get_ref()) as instance:
            step_execution_plan = create_execution_plan(
                job=step_context.job,
                run_config=step_context.run_config,
                step_keys_to_execute=[step_key],
                known_state=active_execution.get_known_state(),
                repository_load_data=job_execution_plan.repository_load_data,
            )

            yield from execute_plan_iterator(
                step_execution_plan,
                step_context.job,
                step_context.dagster_run,
                run_config=step_context.run_config,
                retry_mode=self.retries.for_inner_plan(),
                instance=instance,
            )

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
        job = plan_context.reconstructable_job

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
        # Lock for thread-safe operations
        execution_lock = threading.RLock()

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
                        active_execution.mark_interrupted()

                    # Submit new steps if not stopping
                    if not stopping:
                        # Get steps ready for execution
                        with execution_lock:
                            steps_to_execute = active_execution.get_steps_to_execute(
                                limit=(self._max_concurrent - len(futures))
                            )

                            yield from active_execution.concurrency_event_iterator(plan_context)

                            if not steps_to_execute:
                                break

                            # Submit new steps to thread pool
                            for step in steps_to_execute:
                                step_key = step.key
                                step_context = plan_context.for_step(step)

                                future = thread_pool.submit(
                                    self.execute_step_in_thread,
                                    step_context,
                                    step_key,
                                    active_execution,
                                    execution_plan,
                                )
                                futures[step_key] = future

                                yield DagsterEvent.engine_event(
                                    step_context,
                                    "Starting step in thread",
                                    EngineEventData.in_process(os.getpid()),
                                )

                        # Process any concurrency-related events

                    # Process completed futures (both success and error cases)
                    completed_steps = []
                    for step_key, future in list(futures.items()):
                        if not future.done():
                            continue

                        try:
                            events = future.result()
                            error: ExceptionInfo = cast("ExceptionInfo", future.exception())
                            completed_steps.append(step_key)

                            if error:
                                # Handle step failure
                                step_context = plan_context.for_step(
                                    active_execution.get_step_by_key(step_key)
                                )
                                serializable_error = serializable_error_info_from_exc_info(error)
                                yield DagsterEvent.engine_event(
                                    step_context,
                                    "Step failed in thread execution",
                                    EngineEventData.engine_error(serializable_error),
                                )

                                # Generate failure or retry event
                                failure_or_retry_event = (
                                    self.get_failure_or_retry_event_after_crash(
                                        step_context,
                                        serializable_error,
                                        active_execution.get_known_state(),
                                    )
                                )
                                yield failure_or_retry_event
                                active_execution.handle_event(failure_or_retry_event)

                                errors[step_key] = serializable_error
                            else:
                                # Process step success events
                                for event in events:
                                    yield event
                                    active_execution.handle_event(event)

                        except Exception:
                            # Handle unexpected failures in thread execution
                            error_info = serializable_error_info_from_exc_info(sys.exc_info())
                            step_context = plan_context.for_step(
                                active_execution.get_step_by_key(step_key)
                            )
                            yield DagsterEvent.engine_event(
                                step_context,
                                "Unexpected error in thread execution",
                                EngineEventData.engine_error(error_info),
                            )
                            failure_event = DagsterEvent.step_failure_event(
                                step_context=step_context,
                                step_failure_data=StepFailureData(
                                    error=error_info, user_failure_data=None
                                ),
                            )
                            yield failure_event
                            active_execution.handle_event(failure_event)
                            errors[step_key] = error_info

                        finally:
                            # Clean up completed step
                            active_execution.verify_complete(plan_context, step_key)

                    # Remove completed futures
                    for step_key in completed_steps:
                        del futures[step_key]

                    # Process skipped and abandoned steps
                    yield from active_execution.plan_events_iterator(plan_context)

                    # Sleep briefly if no steps completed to avoid CPU spin
                    if not completed_steps and futures:
                        time.sleep(0.01)

            except Exception:
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

                raise

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
