"""
sentinel/graph/build.py

The only file that imports every node. Wires the StateGraph, all edges
(including conditional ones), and compiles with a checkpointer so
runs are durable and resumable via thread_id = incident_id.

Every node is wrapped with traced_node() here (rather than in each
node file individually) so each one shows up as its own child span
under the incident's root trace in Jaeger.
"""
from langgraph.constants import END, START
from langgraph.graph import StateGraph

from sentinel.graph.state import TriageState
from sentinel.graph.nodes.classify import classify
from sentinel.graph.nodes.evidence import fan_out_evidence, collect
from sentinel.graph.nodes.hypothesize import hypothesize
from sentinel.graph.nodes.dig_deeper import dig_deeper, needs_dig
from sentinel.graph.nodes.propose_action import propose_action
from sentinel.graph.nodes.approval import approval_gate, route_after_approval
from sentinel.graph.nodes.execute import execute
from sentinel.graph.nodes.verify import verify, route_after_verify
from sentinel.graph.nodes.postmortem import postmortem
from sentinel.obs.tracing import traced_node


def build_graph(checkpointer=None):
    g = StateGraph(TriageState)

    g.add_node("classify", traced_node("classify")(classify))
    g.add_node("collect", traced_node("collect")(collect))
    g.add_node("hypothesize", traced_node("hypothesize")(hypothesize))
    g.add_node("dig_deeper", traced_node("dig_deeper")(dig_deeper))
    g.add_node("propose_action", traced_node("propose_action")(propose_action))
    g.add_node("approval", traced_node("approval")(approval_gate))
    g.add_node("execute", traced_node("execute")(execute))
    g.add_node("verify", traced_node("verify")(verify))
    g.add_node("postmortem", traced_node("postmortem")(postmortem))

    g.add_edge(START, "classify")

    # fan_out_evidence returns either END (duplicate) or a list of Send()
    # objects — one per evidence collector — which LangGraph dispatches
    # in parallel, all landing in the "collect" node. This routing
    # function itself is untraced (it's pure logic, not I/O), only the
    # nodes it points at are wrapped above.
    g.add_conditional_edges("classify", fan_out_evidence, ["collect", END])
    g.add_edge("collect", "hypothesize")

    g.add_conditional_edges("hypothesize", needs_dig, ["dig_deeper", "propose_action"])
    g.add_edge("dig_deeper", "hypothesize")

    g.add_edge("propose_action", "approval")
    g.add_conditional_edges("approval", route_after_approval, ["execute", "postmortem"])

    g.add_edge("execute", "verify")
    g.add_conditional_edges("verify", route_after_verify, ["hypothesize", "postmortem"])

    g.add_edge("postmortem", END)

    return g.compile(checkpointer=checkpointer)