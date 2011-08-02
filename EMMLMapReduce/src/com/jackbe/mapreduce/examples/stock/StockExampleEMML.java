package com.jackbe.mapreduce.examples.stock;

/**
 * @author Christopher Steel - JackBe
 *
 * @since Jul 28, 2011 3:46:02 PM
 * @version 1.0
 */
public class StockExampleEMML {

	public static String mapScript = 
			"<mashup xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'  "
			+ "xmlns='http://www.openmashup.org/schemas/v1.0/EMML' "
			+ "xmlns:rdf='http://www.w3.org/1999/02/22-rdf-syntax-ns#' "
			+ "xmlns:ns='http://purl.org/rss/1.0/' "
			+ "name='CSVParser'> "
			+ " <input name='value' type='string' />"
			+ "	<output name='result' type='string' /> "
			+ " <assign fromexpr=\"tokenize(xs:string($value),',')\"        outputvariable='quoteInfo' />"
			+ " <assign fromexpr=\"tokenize(xs:string($quoteInfo[1]),' ')\" outputvariable='timedate'  />"
			+ "	<constructor outputvariable='result'> "
			+ "    <quote>"
			+ "       <symbol>{$quoteInfo[2]}</symbol>"
			+ "       <price>{$quoteInfo[3]}</price>"
			+ "       <date>{$timedate[1]}</date>"
			+ "       <time>{$timedate[2]}</time>"
			+ "       <MapReduceKey>{$quoteInfo[2]}</MapReduceKey>"
			+ "    </quote>"
			+ " </constructor>"
			+ "</mashup>";
					
		public static String redScript = 
			"<mashup xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'  "
			+ "xmlns='http://www.openmashup.org/schemas/v1.0/EMML' "
			+ "xmlns:rdf='http://www.w3.org/1999/02/22-rdf-syntax-ns#' "
			+ "xmlns:ns='http://purl.org/rss/1.0/' "
			+ "name='StockQuoteTransformer'> "
			+ "	<output name='result' type='string' /> "
			+ " <input name='value' type='string' />"
			+ " <assign fromvariable='value' outputvariable='result' type='string'/>"
			+ "</mashup>";
		
}
