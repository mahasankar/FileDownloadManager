package com.mahadev.filedownloadmanager;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.net.MalformedURLException;

import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

public class FileDownloadManager 
{
	/**
	 * Max number of parallel HTTP connections to spawn to download the file.
	 */
	private int maxHttpConnections;
	
	/**
	 * URL to download
	 */
	private String url;
	
	/**
	 * Destination Folder.
	 */
	private String destinationFolder;
	
	/**
	 * Destination Filename.
	 */
	private String destinationFileName;
	
	
	/**
	 * Splits the file to download into multiple chunks using HTTP range request.
	 */
	private Map<String, byte[]> rangeRequestMap = new LinkedHashMap<String, byte[]>();
	
	/**
	 * 
	 * @param maxHttpConnections 
	 * @param url
	 * @param destinationFolder
	 * @param destinationFileName
	 */
	public FileDownloadManager(int maxHttpConnections, String url, String destinationFolder, String destinationFileName)
	{
		this.maxHttpConnections  = maxHttpConnections;
		this.url                 = url;
		this.destinationFolder   = destinationFolder;
		this.destinationFileName = destinationFileName;
	}
	
	/**
	 * Gets the total content length of the given URL
	 * and then splits the content to download into multiple chunks
	 * based on the number of parallel HTTP connections to make.
	 * Stores the request as byte ranges in a Map to
	 * be processed by FileDownloadTask.
	 * 
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	private void initRangeRequest() throws MalformedURLException, IOException
	{
		int contentLength   = FileDownloadUtil.getContentLengthForURL(url);
		
		System.out.println("Total Bytes to Download -> " + contentLength + "\n");
		
		int rangeSize       = contentLength / maxHttpConnections;
		int remainingLength = contentLength % maxHttpConnections;
		
		int startRange      = 0;
		int endRange        = rangeSize - 1;
		
		rangeRequestMap.put("bytes=" + startRange + "-" + endRange, null);
		
		for (int index = 1; index < maxHttpConnections; index++)
		{
			startRange = startRange + rangeSize;
			
			if (index == maxHttpConnections - 1)
			{
				endRange = endRange + rangeSize + remainingLength;
			}
			else
			{
				endRange = endRange + rangeSize;
			}
			
			rangeRequestMap.put("bytes=" + startRange + "-" + endRange, null);
		}
	}
	
	/**
	 * Merge the bytes downloaded in parallel as multiple chunks and write to the 
	 * specified destination folder and filename.
	 * 
	 * @throws IOException
	 */
	private void mergeContents() throws IOException
	{
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		
		for (String nextRange: rangeRequestMap.keySet())
		{
			byte[] nextRangeContent = rangeRequestMap.get(nextRange);
			outputStream.write(nextRangeContent);
		}
		
		Files.write(Paths.get(destinationFolder + destinationFileName), outputStream.toByteArray());
	}
	
	/**
	 * Entry point for FileDownloadManager.
	 * 
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	public void downloadFile() throws MalformedURLException, IOException
	{
		// For the file to be downloaded in chunks, the server
		// must support HTTP range requests.
		FileDownloadUtil.isHttpRangeRequestSupported(url);
				
		initRangeRequest();
		
		// Initialize a ThreadPool to spawn multiple HTTP connections in parallel, 
		// as specified by maxHttpConnections property.
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(maxHttpConnections);
		
		List<Future<Map<String, byte[]>>> resultList = new ArrayList<Future<Map<String, byte[]>>>();
		
		for (String nextRange : rangeRequestMap.keySet())
		{
			System.out.println("Submitting task for range ->" + nextRange);
			FileDownloadTask fileDownloadTask = new FileDownloadTask(url, nextRange);
			
			// Submit the individual HTTP range request to execute as a Callable Future Task.
			Future<Map<String, byte[]>> result = executor.submit(fileDownloadTask);
			resultList.add(result);
		}
		
		System.out.println("\n");
		
		for(Future<Map<String, byte[]>> future : resultList)
        {
			try 
			{
				Map<String, byte[]> result = future.get();
				
				for (String nextRange : result.keySet())
				{
					// Check the status on each Callable Future Task and make sure it was successful.
					// Extract the downloaded bytes for each range request and insert into the Map.
					byte[] nextRangeContent = result.get(nextRange);
					System.out.println("Task successfully run for range request " + nextRange + "? " + future.isDone() + ", Bytes Downloaded = " + nextRangeContent.length);
					rangeRequestMap.put(nextRange, nextRangeContent);
				}
			} 
			catch (InterruptedException | ExecutionException e) 
			{
				e.printStackTrace();
			} 
        }
		
		executor.shutdown();
		
		// Merge the downloaded chunks into one single file.
		mergeContents();
	}
	
	public static void main(String[] args) throws Exception
	{
		// Sample URL's to download
		// https://notepad-plus-plus.org/repository/7.x/7.5.9/npp.7.5.9.Installer.x64.exe
		// http://download.springsource.com/release/STS4/4.0.1.RELEASE/dist/e4.9/spring-tool-suite-4-4.0.1.RELEASE-e4.9.0-win32.win32.x86_64.zip
		
		// Sample Usage
		// java -DmaxHttpConnections=25 -Durl=http://download.springsource.com/release/STS4/4.0.1.RELEASE/dist/e4.9/spring-tool-suite-4-4.0.1.RELEASE-e4.9.0-win32.win32.x86_64.zip -DdestinationFolder=G:\mahadev\temp\ -DdestinationFileName=spring-tool-suite-4-4.0.1.RELEASE-e4.9.0-win32.win32.x86_64_2.zip com.mahadev.filedownloadmanager.FileDownloadManager
		
		String sampleUsage         = "Usage: java -DmaxHttpConnections=25 -Durl=https://notepad-plus-plus.org/repository/7.x/7.5.9/npp.7.5.9.Installer.x64.exe -DdestinationFolder=G:\\mahadev\\temp\\ -DdestinationFileName=npp.7.5.9.Installer.x64_2.exe com.mahadev.filedownloadmanager.FileDownloadManager";
		
		String url                 = System.getProperty("url");
		String destinationFolder   = System.getProperty("destinationFolder");
		String destinationFileName = System.getProperty("destinationFileName");
		
		if (url == null || destinationFolder == null || destinationFileName == null)
		{
			System.out.println(sampleUsage);
			return;
		}
		
		int maxHttpConnections = 1;
		if (System.getProperty("maxHttpConnections") != null)
		{
			maxHttpConnections = Integer.parseInt(System.getProperty("maxHttpConnections"));
		}
		
		FileDownloadManager fileDownloadManager = new FileDownloadManager(maxHttpConnections, url, destinationFolder, destinationFileName);
		long startTime = System.currentTimeMillis();
		fileDownloadManager.downloadFile();
		long endTime = System.currentTimeMillis();
		
		String checksumMultipartDownload = null;
		
		if (maxHttpConnections > 1)
		{
			checksumMultipartDownload = FileDownloadUtil.getMD5Checksum(destinationFolder + destinationFileName);
			System.out.println("Multipart download complete. Time taken to download the file -> " + (endTime - startTime)/1000 + " seconds.");
		}
		else
		{
			System.out.println("Singlepart download complete. Time taken to download the file -> " + (endTime - startTime)/1000 + " seconds.");
		}
		
		// If the request is for an optimized download (i.e maxHttpConnections > 1),
		// make sure we do a Singlepart download and verify that the
		// MD5 checksums for the Singlepart and Multipart downloads match and make
		// sure that the multipart download executed without any data corruption.
		if (maxHttpConnections > 1)
		{
			String originalFileName = "org_" + destinationFileName;
			fileDownloadManager = new FileDownloadManager(1, url, destinationFolder, originalFileName);
			startTime = System.currentTimeMillis();
			fileDownloadManager.downloadFile();
			endTime = System.currentTimeMillis();
			
			System.out.println("Singlepart download complete. Time taken to download the file -> " + (endTime - startTime)/1000 + " seconds.");
			
			String checksumSinglepartDownload = FileDownloadUtil.getMD5Checksum(destinationFolder + originalFileName);
			
			System.out.println("MD5 Checksum for Singlepart and Multipart downloads identical? " + checksumMultipartDownload.equals(checksumSinglepartDownload));
		}
	}
}
