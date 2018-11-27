package com.mahadev.filedownloadmanager;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;

import javax.net.ssl.HttpsURLConnection;
import javax.xml.bind.DatatypeConverter;

public class FileDownloadUtil 
{
	/**
	 * Returns true if the given URL is a HTTPS request, false otherwise.
	 * 
	 * @param url
	 * 
	 * @return boolean
	 */
	public static boolean isHttpsRequest(String url)
	{
		if (url.toLowerCase().startsWith(FileDownloadConstants.HTTPS_URL_PREFIX))
		{
			return true;
		}
		
		return false;
	}
	
	/**
	 * Request true if the given URL supports HTTP range requests, false otherwise.
	 * 
	 * @param url
	 * 
	 * @return boolean
	 * 
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	public static boolean isHttpRangeRequestSupported(String url) throws MalformedURLException, IOException
	{
		HttpURLConnection httpUrlConn =  (HttpURLConnection) (new URL(url)).openConnection();
		
		if (isHttpsRequest(url))
		{
			httpUrlConn = (HttpsURLConnection) httpUrlConn;
		}
		
		httpUrlConn.setRequestMethod(FileDownloadConstants.HTTP_GET_REQUEST);
		httpUrlConn.addRequestProperty(FileDownloadConstants.HTTP_REQUEST_PROPERTY_RANGE, "bytes=0-10");
		
		String acceptRanges = httpUrlConn.getHeaderField(FileDownloadConstants.ACCEPT_RANGES_HEADER);
		
		System.out.println("Accept-Ranges -> " + acceptRanges + "\n");
		
		httpUrlConn.disconnect();
		
		if ("bytes".equals(acceptRanges))
			return true;
		
		return false;
	}
	
	/**
	 * Makes a HTTP HEAD request to the supplied URL and retrieves the content length.
	 * 
	 * @return int 
	 * 
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	public static int getContentLengthForURL(String url) throws MalformedURLException, IOException
	{
		HttpURLConnection httpUrlConn = (HttpURLConnection) (new URL(url)).openConnection();
		
		if (isHttpsRequest(url))
		{
			httpUrlConn = (HttpsURLConnection) httpUrlConn;
		}
		
		httpUrlConn.setRequestMethod(FileDownloadConstants.HTTP_HEAD_REQUEST);
		
		httpUrlConn.disconnect();
		
		return httpUrlConn.getContentLength();
	}
	
	/**
	 * Compute and return the MD5 checksum for the file on disk indicated by
	 * the path parameter.
	 * 
	 * @param path
	 * 
	 * @return
	 * 
	 * @throws Exception
	 */
	public static String getMD5Checksum(String path) throws Exception 
	{
		byte[] byteArr = Files.readAllBytes(Paths.get(path));
		byte[] hash = MessageDigest.getInstance("MD5").digest(byteArr);
		
		return DatatypeConverter.printHexBinary(hash);
	}
	
}
