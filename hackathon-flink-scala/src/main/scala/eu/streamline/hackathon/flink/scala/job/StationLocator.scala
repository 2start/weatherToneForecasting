package eu.streamline.hackathon.flink.scala.job

import eu.streamline.hackathon.common.data.GDELTEvent

import scala.io.Source

class StationLocator extends Serializable {
  val path = "/ghcnd-stations.txt"
  var stations: Vector[Station] = Vector.empty
  readStations()
  stations = stations.filter(station => station.id.startsWith("USC"))

  def distance(station: Station, station2: Station): Double = {
    distance(station.latitude, station.longitude, station2.latitude, station2.longitude)
  }

	def distance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
		val theta = lon1 - lon2
		var dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta))
		dist = Math.acos(dist)
		dist = rad2deg(dist)
		dist * 60 * 1.1515 * 1.609344
	}

	/*:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::*/
	/*::	This function converts decimal degrees to radians						 :*/
	/*:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::*/
	def deg2rad(deg: Double): Double = {
		deg * Math.PI / 180.0
	}

	/*:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::*/
	/*::	This function converts radians to decimal degrees						 :*/
	/*:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::*/
	def rad2deg(rad: Double) : Double ={
		rad * 180 / Math.PI
	}

  def readStations() = {
    val stream = getClass.getResourceAsStream(path)
    val lines = Source.fromInputStream(stream)
    lines.getLines().foreach(line => {
      val fields = line.split(" +")
      stations = stations :+ Station(fields(0), fields(1).toDouble, fields(2).toDouble)
    })
  }

  def nearest(gdelEvent: GDELTEvent): Station = {
    var minDist: Double = Double.MaxValue
    var station: Station = stations.head
    for(s <- stations) {
      val currentDist = distance(s.latitude, s.longitude, gdelEvent.eventGeo_lat, gdelEvent.eventGeo_long)
      if(currentDist < minDist) {
        station = s
        minDist = currentDist
      }
    }
    station
  }
}

case class Station(id: String, latitude: Double, longitude: Double)
