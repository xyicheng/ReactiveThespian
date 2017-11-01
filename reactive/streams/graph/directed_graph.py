'''
Graph Builder for Creating and Maintaining a
directed graph.

Created on Oct 31, 2017

@author: aevans
'''

from multiprocessing import cpu_count
from reactive.actor_system import system_type
from thespian.actors import ActorSystem


class DirectedGraphBuilder():
    """
    Creates a directed graph. The graph is maintained in serveral maps.
    This keeps it directed. It is impossible to overwrite changes
    without causing a 
    """

    def __init__(self, system_type=system_type.multiproc_queue_base()):
        """
        Constructor
        """
        self.__system_type = system_type
        self.__system = ActorSystem(system_type)
        self.sinks = []
        self.nodes = {}
        self.edges = {}

    def get_system(self):
        """
        Get the system
        """
        return self.__system

    def get_system_type(self):
        """
        Get the system type
        """
        return self.__system_type

    def create_process_node(self, klass, edge_nodes=[]):
        """
        Create a process stage.

        :param klass: The node class
        :type klass: Actor
        :param edge_nodes: The edge actor refs
        :type edge_nodes: list() 
        """
        pass

    def create_router_node(self, klass, edge_nodes=[], num_workers=cpu_count()):
        """
        Create a new router node.

        :param klass: The node class
        :type klass: Actor
        :param edge_nodes: List of edge Actors
        :type edge_nodes: list()
        """
        actor = self.__system.createActor(klass)
        for node in edge_nodes:
            self.create_edge(actor, node)

    def create_source(self, klass, edge_nodes=[]):
        """
        Create a source

        :param klass: The class
        :type klass: Source()
        :param edge_nodes: The edge nodes
        :type edge_nodes: list()
        """
        actor = self.__system.createActor(klass)
        for node in edge_nodes:
            self.create_edge(actor, node)

    def create_edge(self, actor, edge_node):
        """
        Create and add an edge.

        :param actor: The actor node
        :type actor: BaseActor
        :param edge_node: The edge node
        :type edge_node: BaseActor
        """
        pass

    def create_sink(self, klass, edge_nodes=[]):
        """
        Create a sink

        :param klass: The sink class
        :type klass: Sink()
        :param edge_nodes: Actor edges
        :type edge_nodes: list()
        """
        pass

    def add_source(self, source):
        """
        Add a source.

        :param source: The source to use
        :type source: Actor
        """
        if source not in self.sources.keys():
            pass

    def add_graph_node(self, node, edge_nodes=[]):
        """
        Add a graph node

        :param node: The graph node to use
        :type node: Actor
        :param edge_nodes: The edges in the graph
        :type edge_nodes: The edges in the node
        """
        if node not in self.stages:
            pass

    def attach_sink(self, sink):
        """
        Attach a sink.

        :param sink: The sink to use
        :type sink: Actor
        """
        if sink not in self.sinks.key():
            pass

    def start(self):
        """
        Start the node.
        """
        pass

    def remove_edge(self, edge_from, edge_to):
        """
        Remove edges from the graph.
        """
        pass

    def shutdown(self):
        """
        Shutdown the graph.
        """
        pass
