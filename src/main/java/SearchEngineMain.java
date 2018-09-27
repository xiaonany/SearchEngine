import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import opennlp.tools.stemmer.snowball.SnowballStemmer;
import opennlp.tools.stemmer.snowball.SnowballStemmer.ALGORITHM;
import searchengine.SearchCore;
import spark.ModelAndView;
import spark.Spark;
import spark.template.jade.JadeTemplateEngine;

public class SearchEngineMain {

	protected static LRUCache<String, WebPage> pageCache;

	public SearchEngineMain(Map<String, Object> conf) {
		Spark.setPort((Integer) conf.get("port"));
		Spark.staticFileLocation("/public");

		// For test
		Spark.get("/test", (req, res) -> "HelloWorld");

		/**
		 * Just an example The hello.jade template file is in the resources/templates
		 * directory
		 */
		Spark.get("/hello", (rq, rs) -> {
			Map<String, String> mes = new HashMap<String, String>();
			mes.put("mess", "World");
			return new ModelAndView(mes, "hello");
		}, new JadeTemplateEngine());

		Spark.get("/", (rq, rs) -> {
			return new ModelAndView(new HashMap<String, String>(), "index");
		}, new JadeTemplateEngine());

		Spark.get("/search/:kw/:head", (rq, rs) -> {
			Map<String, String> params = rq.params();
			String keywork = params.get(":kw");
			String headStr = params.get(":head");
			String kw = URLDecoder.decode(keywork, "utf-8");
			int head = 0;
			try {
				head = Integer.parseInt(headStr);
			} catch (Exception e) {
				rs.status(400);
				return "Bad Request!";
			}
			System.out.println("search for keywords: " + kw);

			ArrayList<String> query = new ArrayList<String>();
			String[] kws = kw.split(" ");
			SnowballStemmer stemmer = new SnowballStemmer(ALGORITHM.ENGLISH);
			for (String k : kws)
				query.add((String) stemmer.stem(k));

			ArrayList<String> result = SearchCore.search(query);
			for (String s:result) {
				System.out.println(s);
			}
			String resp = renderContent(result, head%7*7, 7);

			rs.type("application/json");
			return resp;
		});

	}

	private String renderContent(ArrayList<String> urls, int start, int pageSize) {
		System.out.println("Rendering content...");
		int n = urls.size();
		if (start > n) {
			start = Math.max(0, urls.size() - n);
		}
		StringBuilder json = new StringBuilder();
		json.append("{\"total\":"+urls.size()+",\"pages\":[");

		while (start < n && pageSize > 0) {
			String url = urls.get(start).split(" ")[0];
			WebPage page = null;

			if (pageCache.containsKey(url)) {
				page = pageCache.get(url);
			} else {
				page = SearchCore.fetchPage(url);
				pageCache.put(url, page);
			}
			start++;
			pageSize--;

			if (page == null) continue;
			
			json.append("{\"url\":\"");
			// append url
			json.append(page.getUrl());

			json.append("\",\"title\":\"");
			// append title
			json.append(page.getTitle().replaceAll("\"", ""));

			json.append("\",\"sample\":\"");
			// append sample
			json.append(page.getSample().replaceAll("\"", ""));

			json.append("\"},");
			
		}
		if (json.charAt(json.length()-1) == ',')
			json.deleteCharAt(json.length()-1);
		json.append("]}");
		return json.toString();
	}

	public static void main(String[] args) {
		pageCache = new LRUCache<String, WebPage>(2048);
		
		try {
			System.out.println("Loading page rank ...");
			SearchCore.init("pr_result.txt");
		} catch (NumberFormatException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Cannot load page rank.");
			System.exit(0);
		}
		
		SearchCore.initiate();

		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put("port", Integer.parseInt(args[0]));
		new SearchEngineMain(conf);
		
	}
}
