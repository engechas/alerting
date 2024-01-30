/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.ResourceNotFoundException
import org.opensearch.action.ActionRequest
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.MonitorRunnerService
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.workflow.CompositeWorkflowRunner
import org.opensearch.client.Client
import org.opensearch.common.inject.Inject
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.GetWorkflowRequest
import org.opensearch.commons.alerting.action.GetWorkflowResponse
import org.opensearch.commons.alerting.action.RunWorkflowRequest
import org.opensearch.commons.alerting.action.RunWorkflowResponse
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.rest.RestRequest
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.time.Instant

private val log = LogManager.getLogger(TransportRunWorkflowAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportRunWorkflowAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val transportGetWorkflowAction: TransportGetWorkflowAction
) : HandledTransportAction<ActionRequest, RunWorkflowResponse>(
    AlertingActions.RUN_WORKFLOW_ACTION_NAME, transportService, actionFilters,
    ::RunWorkflowRequest
) {

    override fun doExecute(task: Task, request: ActionRequest, actionListener: ActionListener<RunWorkflowResponse>) {
        // TODO ser/de here is a hack to avoid a ClassCastException. Security Analytics and Alerting both use the RunWorkflowRequest object
        // but have different ClassLoaders - https://stackoverflow.com/a/826345
        val outputStream = BytesStreamOutput()
        request.writeTo(outputStream)
        val actualRequest = RunWorkflowRequest(outputStream.copyBytes().streamInput())

        scope.launch {
            val getWorkflowResponse: GetWorkflowResponse =
                transportGetWorkflowAction.client.suspendUntil {
                    val getWorkflowRequest = GetWorkflowRequest(actualRequest.workflowId, RestRequest.Method.GET)
                    execute(AlertingActions.GET_WORKFLOW_ACTION_TYPE, getWorkflowRequest, it)
                }

            if (getWorkflowResponse.workflow != null) {
                try {
                    // TODO - is using monitorCtx like this safe?
                    // TODO - is Instant.now() fine?
                    val workflowRunResult = CompositeWorkflowRunner.runWorkflow(
                        getWorkflowResponse.workflow!!,
                        MonitorRunnerService.monitorCtx,
                        Instant.now(),
                        Instant.now(),
                        false,
                        actualRequest.documents
                    )

                    if (workflowRunResult.error != null) {
                        actionListener.onFailure(workflowRunResult.error)
                    } else {
                        actionListener.onResponse(RunWorkflowResponse(RestStatus.OK))
                    }
                } catch (e: Exception) {
                    actionListener.onFailure(
                        AlertingException.wrap(e)
                    )
                }
            } else {
                actionListener.onFailure(
                    AlertingException.wrap(
                        ResourceNotFoundException("Workflow with id ${actualRequest.workflowId} not found", RestStatus.NOT_FOUND)
                    )
                )
            }
        }
    }
}
