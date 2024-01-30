package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.client.node.NodeClient
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.RunWorkflowRequest
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener

class RestRunWorkflowAction : BaseRestHandler() {
    private val log = LogManager.getLogger(javaClass)

    override fun getName(): String {
        return "run_workflow_action"
    }

    override fun routes(): List<RestHandler.Route> {
        return listOf(
            RestHandler.Route(
                RestRequest.Method.POST,
                "${AlertingPlugin.WORKFLOW_BASE_URI}/{workflowID}/run"
            )
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.WORKFLOW_BASE_URI}/{workflowID}/run")

        val xcp = request.contentParser()
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
        val runWorkflowRequest = RunWorkflowRequest.parse(xcp)

        return RestChannelConsumer {
                channel ->
            client.execute(
                AlertingActions.RUN_WORKFLOW_ACTION_TYPE,
                runWorkflowRequest,
                RestToXContentListener(channel)
            )
        }
    }
}
