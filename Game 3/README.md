# Game 3: Number of agents at-most d<sub>cutoff</sub> away

This algorithm computes centrality by considering the fringe as the set of all the nodes that are within the distance of d<sub>cutoff</sub> from the node. It has a running time of O(V E + V<sup>2</sup> log(v)), where V = total vertices and E = total edges. For details, please see section `3.4` of the publication.
