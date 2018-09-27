/*
 * Call init(file_path) with pagerank result file first! Call it only once!
 * Then each time call search(), 
 * 	input: Arraylist of String, each is a keyword
 * 	output: Arraylist of String, each is url[space]weight, limit top 200
 */

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;

import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.services.s3.transfer.TransferManager;

import WebPage;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Iterator;
import java.util.List;

import javax.swing.text.html.parser.Element;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

/**
 * Hello world!
 *
 */
public class SearchCore {
	// Static: pagerank table
	public static HashMap<String, Double> pagerank = new HashMap<String, Double>();
	public static boolean init_ed = false;
	static private AmazonS3 s3;
	static private AmazonS3 s3Doc;
	public static SparkConf sparkConf;
	public static JavaSparkContext context;

	// s3
	@SuppressWarnings("deprecation")
	public static void initiate() {
		AWSCredentials credentials = null;
		try {
			credentials = new BasicAWSCredentials("test", "test");
		} catch (Exception e) {
			System.out.println("credentials invalid!");
		}
		s3 = new AmazonS3Client(credentials);
		Region usStandard = Region.getRegion(Regions.US_EAST_1);
		s3.setRegion(usStandard);

		try {
			credentials = new BasicAWSCredentials("AKIAI47DLVDOC76U7XAQ", "x8/vLvuePAi/9+9gl9WdGCMe4gRFvJY6bAMLYS14");
		} catch (Exception e) {
			System.out.println("credentials invalid!");
		}
		s3Doc = new AmazonS3Client(credentials);
		s3Doc.setRegion(usStandard);
	}

	// Load pagerank table
	public static void init(String file_path) throws NumberFormatException, IOException {
		if (init_ed)
			return;
		sparkConf = new SparkConf().setAppName("PageRank").setMaster("local[4]").set("spark.executor.memory", "1g");
		context = new JavaSparkContext(sparkConf);
		/*
		 * 
		 * ____ ___ _ _ _____ / ___/ _ \| \ | | ___| | | | | | | \| | |_ | |__| |_| | |\
		 * | _| \____\___/|_| \_|_| _ ___ ___ _ __ | |_ _____ __ / __/ _ \| '_ \| __/ _
		 * \ \/ / | (_| (_) | | | | || __/> < \___\___/|_| |_|\__\___/_/\_\
		 * 
		 */

		String line;
		BufferedReader reader = new BufferedReader(new FileReader(file_path));
		while ((line = reader.readLine()) != null) {
			String[] parts = line.split(" ", 2);
			if (parts.length >= 2) {
				String key = parts[0];
				Double value = Double.parseDouble(parts[1]);
				pagerank.put(key, value);
			} else {
				System.out.println("ignoring line: " + line);
			}
		}
		initiate();
		System.out.println("Success: loaded pagerank file" + file_path);
		init_ed = true;
	}

	public static Double cbweight(Tuple2<String, Double> t) {
		/*
		 * 
		 * 
		 * 
		 * _ _ _ __ _____(_) __ _| |__ | |_ \ \ /\ / / _ \ |/ _` | '_ \| __| \ V V / __/
		 * | (_| | | | | |_ \_/\_/ \___|_|\__, |_| |_|\__| |___/
		 * 
		 * 
		 */
		try {
			String host = new URL(t._1).getHost();
			if (pagerank.containsKey(host)) {
				// System.err.println("pagerank of "+host + " is:"+ pagerank.get(host));
				Double n = pagerank.get(host);
				if (n > 2.718)
					n = Math.log(n);
				return -1 * (Math.log((0.5 + t._2) * n * 10 + 8) + 3);
			} else {
				Double n = 0.15;
				if (n > 2.718)
					n = Math.log(n);
				return -1 * (Math.log((0.5 + t._2) * n * 10 + 8) + 3);
			}
		} catch (MalformedURLException e) {
		}
		return 0.0;
	}

	public static ArrayList<String> search(ArrayList<String> keywords) {
		/*
		 * 
		 * _ ___ ___ __ _ _ __ ___| |__ / __|/ _ \/ _` | '__/ __| '_ \ \__ \ __/ (_| | |
		 * | (__| | | | |___/\___|\__,_|_| \___|_| |_|
		 * 
		 * 
		 */
		// Spark frame

		if (!init_ed) {
			System.err.println("ERROR: un_initiated. Call init(file_path) first");
			return new ArrayList<String>();
		}

		StringBuilder sb = new StringBuilder();
		// read S3 into RDD of keyword
		for (String word : keywords) {
			try {
				S3Object obj = s3Doc.getObject(new GetObjectRequest("index-table-00", word));
				BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(obj.getObjectContent()));
				String line;
				try {
					while ((line = bufferedReader.readLine()) != null) {
						if (line != null && !line.trim().equals("")) {
							sb.append(line + "\n");
							// System.err.println(line);
						}
					}

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (AmazonS3Exception e) {

			}

		}

		String s = sb.toString();
		String[] lines = s.split("(\\r?\\n)|;");
		JavaRDD<String> word_tfidf_raw = context.parallelize(Arrays.asList(lines));
		JavaPairRDD<String, Double> url_tfidf = word_tfidf_raw.mapToPair(new PairFunction<String, String, Double>() {
			public Tuple2<String, Double> call(String s) {

				// s->[0] [1]
				double doub;
				try {

					String safety = s.trim();
					String[] urllink = null;
					if (safety.indexOf(" ") == -1) {
						urllink = new String[2];
						int sp = safety.lastIndexOf(":");
						urllink[0] = safety.substring(0, sp);
						urllink[1] = safety.substring(sp + 1);
					} else {
						safety = safety + " 0.0";
						urllink = safety.split(" ");
					}
					doub = Double.parseDouble(urllink[1]);
					return new Tuple2<String, Double>(urllink[0], doub);
				} catch (Exception e) {
					return new Tuple2<String, Double>("empty", (double) 0.0);
				}
			}
		});

		// transform weight and combine with pagerank
		JavaPairRDD<String, Double> url_weight = url_tfidf
				.mapToPair(t -> new Tuple2<String, Double>(t._1, cbweight(t)));

		// find url weight sum
		JavaPairRDD<String, Double> url_rank = url_weight.reduceByKey((a, b) -> (a + b));

		// sort
		JavaPairRDD<Double, String> rev_url_rank = url_rank.mapToPair(t -> new Tuple2<Double, String>(t._2, t._1));
		rev_url_rank = rev_url_rank.sortByKey();

		JavaRDD<String> str_rank = rev_url_rank.map(t -> t._2 + " " + -1 * t._1);

		/*
		 * 
		 * _ _ ___ _ _| |_ _ __ _ _| |_ / _ \| | | | __| '_ \| | | | __| | (_) | |_| |
		 * |_| |_) | |_| | |_ \___/ \__,_|\__| .__/ \__,_|\__| |_|
		 * 
		 */

		List<String> url_list = str_rank.collect();
		if (url_list.size() > 200)
			url_list = url_list.subList(0, 200);
		ArrayList res = new ArrayList<String>(url_list);

		return res;
	}

	public static WebPage fetchPage(String url) {
		System.out.println("Fetching " + url + "...");
		WebPage page = new WebPage();
		String key = "docs/" + url.replaceAll("/", "_") + ".txt";
		S3Object res = null;
		for (int i = 0; i < 10; i++) {
			try {
				GetObjectRequest greq = new GetObjectRequest("crawler-storage-0" + i, key);

				res = s3Doc.getObject(greq);
				if (res != null)
					break;
			} catch (Exception e) {
				// e.printStackTrace();
			}
		}
		if (res == null)
			return null;

		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(res.getObjectContent()));
			StringBuilder resContent = new StringBuilder();
			String line = null;

			page.setUrl(br.readLine());
			line = br.readLine();
			line = br.readLine();
			line = br.readLine();

			while ((line = br.readLine()) != null) {
				resContent.append(line + "\n");
			}

			String cont = resContent.toString();
			String regEx_script = "<[\\s]*?script[^>]*?>[\\s\\S]*?<[\\s]*?\\/[\\s]*?script[\\s]*?>";
			String regEx_style = "<[\\s]*?style[^>]*?>[\\s\\S]*?<[\\s]*?\\/[\\s]*?style[\\s]*?>";
			cont.replaceAll(regEx_script, "").replaceAll(regEx_style, "");
			Document doc = Jsoup.parse(cont, "", Parser.xmlParser());
			String title = doc.select("title").text();
			page.setTitle(title);
			Elements p = doc.select("p");

			String sample = p.isEmpty() ? "No sample paragraph" : p.first().text();

			page.setSample(sample);

		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

		return page;
	}

	public static void main(String[] args) {
		try {
			init("pr_result.txt");
			// for (String key : pagerank.keySet()) {
			// System.out.println(key + ":" + pagerank.get(key));
			// }

		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String query = "abstract act";
		ArrayList<String> myList = new ArrayList<String>(Arrays.asList(query.split(" ")));
		ArrayList<String> larry = search(myList);

		for (String s : larry) {
			System.out.println(s);
		}

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String query2 = "able act";
		ArrayList<String> myList2 = new ArrayList<String>(Arrays.asList(query2.split(" ")));
		ArrayList<String> larry2 = search(myList2);
		for (String s : larry2) {
			System.out.println(s);
		}
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String query3 = "absent able ddd";
		ArrayList<String> myList3 = new ArrayList<String>(Arrays.asList(query3.split(" ")));
		ArrayList<String> larry3 = search(myList3);
		for (String s : larry3) {
			System.out.println(s);
		}

	}
}
