package com.jackbe.mapreduce.examples.stock;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.oma.emml.utils.XMLConversionUtils;
import org.w3c.dom.Document;

/**
 * @author Christopher Steel - JackBe
 *
 * @since Jul 11, 2011 4:35:25 PM
 * @version 1.0
 */
public class TestConv {

	/**
	 * @param args
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException {
		String filename = "input/stocks.csv";

		@SuppressWarnings("unused")
		XMLConversionUtils xmlUtils = new org.oma.emml.utils.XMLConversionUtils();
		//InputStreamReader reader = new java.io.InputStreamReader(xmlUtils.getClass().getClassLoader().getResourceAsStream(filename));
		FileInputStream reader = new FileInputStream((filename));
        try {
			Document result = XMLConversionUtils.getDocument(reader);
			System.out.println("Doc: " + result);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

}
