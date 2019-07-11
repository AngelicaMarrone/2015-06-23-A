package it.polito.tdp.music.model;

import java.time.Month;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleWeightedGraph;

import it.polito.tdp.music.db.MusicDAO;

public class Model {
	
private MusicDAO dao;

	

	//scelta valore mappa

	private Map<Integer,Country> idMap;

	

	//scelta tipo valori lista

	private List<Country> vertex;

	

	//scelta tra uno dei due edges

	private List<Adiacenza> edges;

	

	//scelta tipo vertici e tipo archi

	private Graph<Country,DefaultWeightedEdge> graph;
	
	private Country source;
	private Country target;
	private Map<String,Integer> classifica;
	private Integer mese;



	public Model() {

		

		//inserire tipo dao

		dao  = new MusicDAO();

		//inserire tipo values

		idMap = new HashMap<Integer,Country>();

	}

	

	public int creaGrafo() {

		

		//scelta tipo vertici e archi

		graph = new SimpleWeightedGraph<Country,DefaultWeightedEdge>(DefaultWeightedEdge.class);
		
		List<Country> nazioni= new ArrayList<Country>();
		
		for (String s: classifica.keySet())
		{
			dao.getCountries(mese,s,nazioni,idMap);
			
		}

		

		//scelta tipo valori lista

		vertex = new ArrayList<Country>(nazioni);

		Graphs.addAllVertices(graph,vertex);
		edges = new ArrayList<Adiacenza>();

		for(Country c: nazioni)
		{ for(Country c2:nazioni)
			{  if(c!=c2)
				dao.getEdges(c, c2,edges,mese);
			}
		}
		
		System.out.println(nazioni);
		for(Adiacenza a : edges) {

			

			//CASO BASE POTRESTI DOVER AGGIUNGERE CONTROLLI

			source = idMap.get(a.getId1());

			target = idMap.get(a.getId2());

			double peso = a.getPeso();

			Graphs.addEdge(graph,source,target,peso);

			System.out.println("AGGIUNTO ARCO TRA: "+source.toString()+" e "+target.toString());

			

		}

		

		System.out.println("#vertici: "+graph.vertexSet().size());

		System.out.println("#archi: "+graph.edgeSet().size());
		
		
		int peso= maxDistanzaCountry();

		return peso;
		

	}
	
	public int maxDistanzaCountry() {

		int max = 0 ;

		

		for( DefaultWeightedEdge e : graph.edgeSet() ) {

			if ( graph.getEdgeWeight(e) > max )

				max = (int)graph.getEdgeWeight(e) ;

		}

		return max ;

	}

	public List<Month> getMonths() {
		List<Month> months= dao.getAllMonths();
		return months;
	}



	public String trovaElenco(int value) {
		String ris= "Elenco artisti e ascolti per il mese selezionato:\n";
		classifica= new LinkedHashMap<String,Integer>();
		dao.getClassifica(value, classifica);
		
		for(String s: classifica.keySet())
		{
			ris+= "- " + s + " " + classifica.get(s)+ "\n";
		}
		
		this.mese=value;
		
		return ris;
		
	}

}
