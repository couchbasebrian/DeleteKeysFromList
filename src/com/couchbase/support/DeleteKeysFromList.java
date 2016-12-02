package com.couchbase.support;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

// December 2, 2016

public class DeleteKeysFromList {

	public static void main(String[] args) {

		long t1 = 0, t2 = 0;

		String host = "", bucketName = "", password = "", inputFileName = "";

		// check to see if we were given arguments

		System.out.println("I see " + args.length + " arguments");

		if (args.length >= 3) {
			inputFileName = args[0];
			host          = args[1];
			bucketName    = args[2];
		} else{ 
			System.out.println("Usage: DeleteKeysFromList <keyFileList> <host> <bucketName> <optional bucket password>");
			System.exit(1);
		}

		System.out.println("I will read from the file:  " + inputFileName);
		System.out.println("I will connect to the host: " + host);
		System.out.println("I will access the bucket:   " + bucketName);

		if (args.length >= 4) {
			System.out.println("Using a bucket password");
			password = args[3];
		} else {
			System.out.println("Not using a bucket password");
		}

		// 10 seconds
		long timeoutInMilliseconds = 10000;

		t1 = System.currentTimeMillis();

		DefaultCouchbaseEnvironment.Builder builder = DefaultCouchbaseEnvironment.builder();
		builder.connectTimeout(timeoutInMilliseconds);
		builder.viewTimeout(timeoutInMilliseconds);
		builder.queryTimeout(timeoutInMilliseconds);
		DefaultCouchbaseEnvironment environment = builder.build();

		Bucket bucket = null;
		Cluster cluster = CouchbaseCluster.create(environment, host);
		if (password == null || password.length() == 0) {
			bucket = cluster.openBucket(bucketName);
		} else {
			bucket = cluster.openBucket(bucketName, password);
		}

		t2 = System.currentTimeMillis();

		System.out.println("Cluster and bucket connections established in " + (t2 - t1) + " ms.");

		// Open the file and read it into memory
		t1 = System.currentTimeMillis();
		ArrayList<String> listOfKeys = getListOfKeysFromFile(inputFileName);
		t2 = System.currentTimeMillis();

		System.out.println("I have read " + listOfKeys.size() + " keys into memory in " + (t2 - t1) + " ms and will start processing them now.");

		boolean keyDeleteSuccess = false;
		long documentsThatShouldBeDeleted = 0, successfulDeletes = 0;

		// Main loop over the keys
		for (int i = 0; i < listOfKeys.size(); i++) {
			String eachKey = listOfKeys.get(i);
			System.out.println("Working on key #" + i + " : "+ eachKey);

			if (shouldDocumentBeDeleted(bucket, eachKey)) {
				documentsThatShouldBeDeleted++;
				keyDeleteSuccess = attemptToDelete(bucket, eachKey);
				if (keyDeleteSuccess) {
					successfulDeletes++;
				}
			}
		}

		System.out.println("Done processing key list.");

		System.out.println("Total number of keys:                  " + listOfKeys.size());
		System.out.println("Number that matched deletion criteria: " + documentsThatShouldBeDeleted);
		System.out.println("Total number successfully deleted:     " + successfulDeletes);

		// Shut down
		System.out.println("Closing Cluster and bucket connections.");
		bucket.close();
		cluster.disconnect();

	} // main()


	// This is where your application specific deletion logic goes
	static boolean shouldDocumentBeDeleted(Bucket bucket, String eachKey) {

		// You could change this to true, which will delete any document on the list
		boolean rval = false;

		// Your document fields go here
		String jsonStringFieldName = "stringPropertyName";
		String jsonIntFieldName    = "intPropertyName";

		// get the document and examine it
		JsonDocument eachDocument = bucket.get(eachKey);

		// First, does this document exist?
		if (eachDocument != null) {
			JsonObject eachJsonObject = eachDocument.content();

			// You might be interested in a string field, or perhaps an integer field
			String s = eachJsonObject.getString(jsonStringFieldName);
			Integer i = eachJsonObject.getInt(jsonIntFieldName);

			// Perhaps we should delete this document if the string matches another string
			if ((s != null) && (s.equals("A certain string"))) {
				rval = true;
			}

			// Perhaps we should delete this document if the integer is above a certain amount
			int aLargeValue = 3342423;
			if (i != null) {
				int iInt = i.intValue();
				if (iInt > aLargeValue) {
					rval = true;
				}
			}

			// Done with checks for this document

		}

		return rval;
	}



	// Return true if we succeed
	static boolean attemptToDelete(Bucket bucket, String eachKey) {

		boolean rval = true;

		try {
			bucket.remove(eachKey);
		}
		catch (Exception e) {
			rval = false;
		}

		return rval;

	}


	static ArrayList<String> getListOfKeysFromFile(String fileName) {

		ArrayList<String> returnValue = new ArrayList<String>();

		BufferedReader br = null;
		FileReader fr = null;

		try {
			fr = new FileReader(fileName);
			br = new BufferedReader(fr);
			String sCurrentLine;
			while ((sCurrentLine = br.readLine()) != null) {
				returnValue.add(sCurrentLine);			
			}
		} catch (IOException e) {
			System.out.println("Exception when trying to read the list of keys from the file.");
			e.printStackTrace();
		} finally {
			try {
				if (br != null) br.close();
				if (fr != null) fr.close();

			} catch (IOException ex) {
				System.out.println("Exception when trying to read the list of keys from the file ( in the finally clause )");
				ex.printStackTrace();
			}
		}

		return returnValue;

	} // getListOfKeysFromFile


} // DeleteKeysFromList

// EOF