package com.couchbase.support;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

// December 2, 2016

public class DeleteKeysFromList {

	public static void main(String[] args) {

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

		DefaultCouchbaseEnvironment.Builder builder = DefaultCouchbaseEnvironment.builder();
		builder.connectTimeout(1000);
		builder.viewTimeout(1000);
		builder.queryTimeout(10);
		DefaultCouchbaseEnvironment environment = builder.build();

		Bucket bucket = null;
		Cluster cluster = CouchbaseCluster.create(environment, host);
		if (password == null || password.length() == 0) {
			bucket = cluster.openBucket(bucketName);
		} else {
			bucket = cluster.openBucket(bucketName, password);
		}

		System.out.println("Cluster and bucket connections established.");

		// Open the file
		ArrayList<String> listOfKeys = getListOfKeysFromFile(inputFileName);

		System.out.println("I have read " + listOfKeys.size() + " into memory and will start processing them now.");

		boolean keyDeleteSuccess = false;
		long documentsThatShouldBeDeleted = 0, successfulDeletes = 0;


		for (int i = 0; i < listOfKeys.size(); i++) {
			String eachKey = listOfKeys.get(i);
			System.out.println("Working on key: " + eachKey);

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


	static boolean shouldDocumentBeDeleted(Bucket bucket, String eachKey) {
		boolean rval = true;

		// get the document and examine it

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
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();

				if (fr != null)
					fr.close();

			} catch (IOException ex) {

				ex.printStackTrace();

			}
		}

		return returnValue;
	}


} // DeleteKeysFromList

// EOF