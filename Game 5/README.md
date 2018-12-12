# Game 5: Number of agents with total weight inside C (coalition) >= W<sub>cutoff</sub> (agent):

This algorithm computes centrality by considering the fringe as the set of all nodes whose agent specific threshold is less than the sum of influences on the node by the nodes who are already in the coalition. It has a running time of O(V + E<sup>2</sup>), where V = total vertices and E = toal edges. For details, please see section `3.6` of the publication.
