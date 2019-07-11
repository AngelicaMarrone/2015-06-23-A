package it.polito.tdp.music.db;

import it.polito.tdp.music.model.Adiacenza;
import it.polito.tdp.music.model.Artist;
import it.polito.tdp.music.model.City;
import it.polito.tdp.music.model.Country;
import it.polito.tdp.music.model.Listening;
import it.polito.tdp.music.model.Track;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.time.Month;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MusicDAO {
	
	public List<Country> getAllCountries(Map<Integer, Country> idMap) {
		final String sql = "SELECT id, country FROM country" ;
		
		List<Country> countries = new LinkedList<Country>() ;
		
		try {
			Connection conn = DBConnect.getConnection() ;

			PreparedStatement st = conn.prepareStatement(sql) ;
			
			ResultSet res = st.executeQuery() ;
			
			while( res.next() ) {
				Country c=new Country(res.getInt("id"), res.getString("country"));
				countries.add( c);
				idMap.put(c.getId(), c);
			}
			
			conn.close() ;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null ;
		}
		
		return countries ;
		
	}
	
	public List<City> getAllCities() {
		final String sql = "SELECT id, city FROM city" ;
		
		List<City> cities = new LinkedList<City>() ;
		
		try {
			Connection conn = DBConnect.getConnection() ;

			PreparedStatement st = conn.prepareStatement(sql) ;
			
			ResultSet res = st.executeQuery() ;
			
			while( res.next() ) {
				cities.add( new City(res.getInt("id"), res.getString("city"))) ;
			}
			
			conn.close() ;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null ;
		}
		
		return cities ;
		
	}

	
	public List<Artist> getAllArtists() {
		final String sql = "SELECT id, artist FROM artist" ;
		
		List<Artist> artists = new LinkedList<Artist>() ;
		
		try {
			Connection conn = DBConnect.getConnection() ;

			PreparedStatement st = conn.prepareStatement(sql) ;
			
			ResultSet res = st.executeQuery() ;
			
			while( res.next() ) {
				artists.add( new Artist(res.getInt("id"), res.getString("artist"))) ;
			}
			
			conn.close() ;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null ;
		}
		
		return artists ;
		
	}

	public List<Track> getAllTracks() {
		final String sql = "SELECT id, track FROM track" ;
		
		List<Track> tracks = new LinkedList<Track>() ;
		
		try {
			Connection conn = DBConnect.getConnection() ;

			PreparedStatement st = conn.prepareStatement(sql) ;
			
			ResultSet res = st.executeQuery() ;
			
			while( res.next() ) {
				tracks.add( new Track(res.getInt("id"), res.getString("track"))) ;
			}
			
			conn.close() ;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null ;
		}
		
		return tracks ;
		
	}
	
	public List<Listening> getAllListenings() {
		final String sql = "SELECT id, userid, month, weekday, longitude, latitude, countryid, cityid, artistid, trackid FROM listening" ;
		
		List<Listening> listenings = new LinkedList<Listening>() ;
		
		try {
			Connection conn = DBConnect.getConnection() ;

			PreparedStatement st = conn.prepareStatement(sql) ;
			
			ResultSet res = st.executeQuery() ;
			
			while( res.next() ) {
				listenings.add( new Listening(res.getLong("id"), res.getLong("userid"), res.getInt("month"), res.getInt("weekday"),
						res.getDouble("longitude"), res.getDouble("latitude"), res.getInt("countryid"), res.getInt("cityid"),
						res.getInt("artistid"), res.getInt("trackid"))) ;
			}
			
			conn.close() ;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null ;
		}
		
		return listenings ;
		
	}
	
	public List<Month> getAllMonths() {
		final String sql = "SELECT DISTINCT MONTH " + 
				"FROM listening " + 
				"ORDER BY MONTH " ;
		
		List<Month> months = new LinkedList<Month>() ;
		
		try {
			Connection conn = DBConnect.getConnection() ;

			PreparedStatement st = conn.prepareStatement(sql) ;
			
			ResultSet res = st.executeQuery() ;
			
			while( res.next() ) {
				
				Month m=  Month.of(res.getInt("month"));
				
				months.add(m ) ;
			}
			
			conn.close() ;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null ;
		}
		
		return months ;
		
	}


	
	
	public static void main(String[] args) {
		MusicDAO dao = new MusicDAO() ;
		
		//List<Country> countries = dao.getAllCountries(idMap) ;
		//System.out.println(countries) ;
		
		List<City> cities = dao.getAllCities() ;
		//System.out.println(cities) ;
		
		List<Artist> artists = dao.getAllArtists() ;
		
		List<Track> tracks = dao.getAllTracks() ;
		
		List<Listening> listenings = dao.getAllListenings() ;



		//System.out.format("Loaded %d countries, %d cities, %d artists, %d tracks, %d listenings\n", 
	//			countries.size(), cities.size(), artists.size(), tracks.size(), listenings.size()) ;
	}

	public void getClassifica(int value, Map<String, Integer> classifica) {
		String sql = "SELECT artist, COUNT(l.id) AS cnt " + 
				"FROM listening l, artist a " + 
				"WHERE l.artistid=a.id " + 
				"AND l.MONTH=? " + 
				"GROUP BY artist " + 
				"ORDER BY cnt desc " + 
				"LIMIT 20 " ;
		
		try {
			Connection conn = DBConnect.getConnection() ;

			PreparedStatement st = conn.prepareStatement(sql) ;
			
			st.setInt(1, value);
			
			ResultSet res = st.executeQuery() ;
			
			while( res.next() ) {
				
				classifica.put(res.getString("artist"), res.getInt("cnt"));
			}
			
			conn.close() ;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return ;
		}
		
	}

	public void getCountries(Integer mese, String s, List<Country> nazioni, Map<Integer, Country> idMap) {
		String sql = "SELECT DISTINCT c.id,c.country " + 
				"FROM listening l, artist a, country c " + 
				"WHERE l.countryid=c.id AND l.artistid=a.id " + 
				"AND l.MONTH=? " + 
				"AND a.artist=? " ;
		
		try {
			Connection conn = DBConnect.getConnection() ;

			PreparedStatement st = conn.prepareStatement(sql) ;
			
			st.setInt(1, mese);
			st.setString(2, s);
			
			ResultSet res = st.executeQuery() ;
			
			while( res.next() ) {
				
				Country c= new Country(res.getInt("c.id"), res.getString("c.country"));
				if (!nazioni.contains(c))
				{
					nazioni.add(c);
					idMap.put(c.getId(), c);
				}
				
			}
			
			conn.close() ;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return ;
		}
		
	}

	public void getEdges(Country c, Country c2, List<Adiacenza> edges, Integer mese) {
		
		String sql = "SELECT l1.countryid, l2.countryid, COUNT(DISTINCT l1.artistid) as cnt " + 
				"FROM listening l1, listening l2 " + 
				"WHERE l1.MONTH=? AND l2.MONTH=l1.MONTH " + 
				"AND l1.artistid=l2.artistid " + 
				"AND l1.countryid>l2.countryid AND l1.countryid=? AND l2.countryid=? " + 
				"GROUP BY l1.countryid, l2.countryid " ;
		
		try {
			Connection conn = DBConnect.getConnection() ;

			PreparedStatement st = conn.prepareStatement(sql) ;
			
			st.setInt(1, mese);
			st.setInt(2, c.getId());
			st.setInt(3, c2.getId());
			
			ResultSet res = st.executeQuery() ;
			
			while( res.next() ) {
				
				Adiacenza a= new Adiacenza(res.getInt("l1.countryid"),res.getInt("l2.countryid"),res.getInt("cnt"));
				if (!edges.contains(a) && a.getPeso()!=0)
					edges.add(a);
			}
			
			conn.close() ;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return ;
		}
		
		
	}

}
