package com.graph;

import org.neo4j.graphdb.RelationshipType;
/**
 * relationType
 * @author Jeff
 *
 */
public enum RelTypes implements RelationshipType{
	Related,Contributed_by,Cite,Co_occurrence,Have,Sim,Assigned_to;
      
}
