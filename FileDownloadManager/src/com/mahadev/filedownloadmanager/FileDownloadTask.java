package com.mahadev.filedownloadmanager;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.net.ssl.HttpsURLConnection;

public class FileDownloadTask implements Callable<Map<String, byte[]>>
{
	private String url;
	
	private String range;
	
	/**
	 * 
	 * @param url
	 * @param range
	 */
	public FileDownloadTask(String url, String range)
	{
		this.url   = url;
		this.range = range;
	}
	
	
	@Override
	/**
	 * Execute the specified HTTP range request for the URL supplied.
	 * Append the bytes downloaded for the range request to the range request map.
	 */
    public Map<String, byte[]> call() throws Exception 
	{
    	URL url = new URL(this.url);
		
		HttpURLConnection httpUrlConn = (HttpURLConnection) url.openConnection();
		
		if (FileDownloadUtil.isHttpsRequest(this.url))
		{
			httpUrlConn = (HttpsURLConnection) httpUrlConn;
		}
		
		httpUrlConn.setRequestMethod(FileDownloadConstants.HTTP_GET_REQUEST);
		httpUrlConn.addRequestProperty(FileDownloadConstants.HTTP_REQUEST_PROPERTY_RANGE, range);
		
		InputStream inputStream = httpUrlConn.getInputStream();
		
		byte[] buffer = new byte[2048];
        
        int length;
        int downloaded = 0; 
        
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        while ((length = inputStream.read(buffer)) != -1) 
        {
            outputStream.write(buffer, 0, length);
            downloaded += length;
        }
        
        Map<String, byte[]> result = new HashMap<String, byte[]>();
		result.put(range, outputStream.toByteArray());
		
		//System.out.println("Total bytes downloaded for range " + range + " -> " + downloaded);
		
		outputStream.close();
        inputStream.close();
		httpUrlConn.disconnect();
		
		return result;
	}
}
