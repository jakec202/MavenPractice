package com.projectfive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class FlightTopology {

	static class GetFlightStatsBolt extends BaseBasicBolt {
		ObjectMapper mapper;
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			mapper = new ObjectMapper();
			super.prepare(stormConf, context);
		}

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String flightJSON = tuple.getString(0);
			FlightInfoExResponse.FlightInfoExResult.Flight flight;
			try {
				flight = mapper.readValue(flightJSON, FlightInfoExResponse.class).getFlightInfoExResult().getFlights()[0];
				int delay = (flight.getActualdeparturetime() - flight.getFiledDeparturetime())/60;
				collector.emit(new Values(flight.getOrigin().substring(1), // Convert KORD to ORD
						                  flight.getDestination().substring(1),
						                  delay));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("origin", "destination", "delay"));
		}

	}

	static class UpdateFlightsBolt extends BaseBasicBolt {
		private org.apache.hadoop.conf.Configuration conf;
		private HConnection hConnection;
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			try {
				conf = HBaseConfiguration.create();
				conf.set("zookeeper.znode.parent", "/hbase-unsecure");
				hConnection = HConnectionManager.createConnection(conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			super.prepare(stormConf, context);
		}

		@Override
		public void cleanup() {
			try {
				hConnection.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// TODO Auto-generated method stub
			super.cleanup();
		}

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			HTableInterface weatherTable = null;
			HTableInterface flightTable = null;
			try {
				String origin = input.getStringByField("origin");
				String destination = input.getStringByField("destination");
				int delay = input.getIntegerByField("delay");
				String route = origin + destination;
				weatherTable = hConnection.getTable("current_weather");
				Get getWeather = new Get(Bytes.toBytes(origin));
				Result weather = weatherTable.get(getWeather);
				if(weather.isEmpty())  // No weather for airport. Punt
					return;
				
				flightTable = hConnection.getTable("weather_delays_by_route"); 
				Increment increment = new Increment(Bytes.toBytes(route));
				if(Bytes.toBoolean(weather.getValue(Bytes.toBytes("weather"), Bytes.toBytes("fog")))) {
					increment.addColumn(Bytes.toBytes("delay"), Bytes.toBytes("fog_flights"), 1);
					increment.addColumn(Bytes.toBytes("delay"), Bytes.toBytes("fog_delays"), delay);
				}
				if(Bytes.toBoolean(weather.getValue(Bytes.toBytes("weather"), Bytes.toBytes("rain")))) {
					increment.addColumn(Bytes.toBytes("delay"), Bytes.toBytes("rain_flights"), 1);
					increment.addColumn(Bytes.toBytes("delay"), Bytes.toBytes("rain_delays"), delay);
				}
				if(Bytes.toBoolean(weather.getValue(Bytes.toBytes("weather"), Bytes.toBytes("snow")))) {
					increment.addColumn(Bytes.toBytes("delay"), Bytes.toBytes("snow_flights"), 1);
					increment.addColumn(Bytes.toBytes("delay"), Bytes.toBytes("snow_delays"), delay);
				}
				if(Bytes.toBoolean(weather.getValue(Bytes.toBytes("weather"), Bytes.toBytes("hail")))) {
					increment.addColumn(Bytes.toBytes("delay"), Bytes.toBytes("hail_flights"), 1);
					increment.addColumn(Bytes.toBytes("delay"), Bytes.toBytes("hail_delays"), delay);
				}
				if(Bytes.toBoolean(weather.getValue(Bytes.toBytes("weather"), Bytes.toBytes("thunder")))) {
					increment.addColumn(Bytes.toBytes("delay"), Bytes.toBytes("thunder_flights"), 1);
					increment.addColumn(Bytes.toBytes("delay"), Bytes.toBytes("thunder_delays"), delay);
				}
				if(Bytes.toBoolean(weather.getValue(Bytes.toBytes("weather"), Bytes.toBytes("tornado")))) {
					increment.addColumn(Bytes.toBytes("delay"), Bytes.toBytes("tornado_flights"), 1);
					increment.addColumn(Bytes.toBytes("delay"), Bytes.toBytes("tornado_delays"), delay);
				}
				if(Bytes.toBoolean(weather.getValue(Bytes.toBytes("weather"), Bytes.toBytes("clear")))) {
					increment.addColumn(Bytes.toBytes("delay"), Bytes.toBytes("clear_flights"), 1);
					increment.addColumn(Bytes.toBytes("delay"), Bytes.toBytes("clear_delays"), delay);
				}
				flightTable.increment(increment);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				if(weatherTable != null)
					try {
						weatherTable.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				if(flightTable != null)
					try {
						flightTable.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub

		}

	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

		String zkIp = "localhost";

		String nimbusHost = "sandbox.hortonworks.com";

		String zookeeperHost = zkIp +":2181";

		ZkHosts zkHosts = new ZkHosts(zookeeperHost);
		List<String> zkServers = new ArrayList<String>();
		zkServers.add(zkIp);
		SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "spertus-flight-events", "/spertus-flights-events","flight_id");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
		kafkaConfig.zkServers = zkServers;
		kafkaConfig.zkRoot = "/spertus-flight-events";
		kafkaConfig.zkPort = 2181;
		kafkaConfig.forceFromStart = true;
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("flight-events", kafkaSpout, 1);
		builder.setBolt("flight-stats", new GetFlightStatsBolt(), 1).shuffleGrouping("flight-events");
		builder.setBolt("update-flights", new UpdateFlightsBolt(), 1).shuffleGrouping("flight-stats");
		Map conf = new HashMap();
		conf.put(backtype.storm.Config.TOPOLOGY_WORKERS, 4);
		conf.put(backtype.storm.Config.TOPOLOGY_DEBUG, true);
		if (args != null && args.length > 0) {
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}   else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("flight-topology", conf, builder.createTopology());
		}
	}
}
