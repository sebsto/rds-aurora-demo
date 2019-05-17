import java.security.InvalidParameterException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Base64;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.amazonaws.services.secretsmanager.*;
import com.amazonaws.services.secretsmanager.model.*;

class RDS {

    private static final String SECRET_NAME = "demo-aurora";
    // private static final String SECRET_NAME = "demo-rds";
    private static final Map<String,Object> SECRET = getSecret(SECRET_NAME);
    
    // build the JDBC URL and parameters from Secret
    private static final String DB_URL = "jdbc:mysql://" + SECRET.get("host") + ":" + SECRET.get("port") + "/employees";
    private static final String DB_USERNAME = SECRET.get("username").toString();
    private static final String DB_PASSWORD = SECRET.get("password").toString();

    public static void main(String args[]) {

        System.out.println("Executing against database : " + SECRET_NAME + " at host " + SECRET.get("host"));
        long start = System.currentTimeMillis();
        
        try {
            
            while(true) {
                // queryDatabase();
                doOperationAndRetry();
                Thread.sleep(2000);
            }
            
        } catch (Exception ex) {
            System.out.println("Failed to invoke DB " + ex);
        }
        
        long finish = System.currentTimeMillis();        
        System.out.println("Execution time " + (finish - start) + " ms");
    }
    
    public static void queryDatabase() throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection con = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD);
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from employees");
        while (rs.next()) {
            // System.out.println(rs.getInt(1) + "  " + rs.getString(2) + "  " + rs.getString(3));
            System.out.println("There are " + rs.getInt(1) + " employees in the database");
        }
        con.close();
    }
    
    /***************************************************************************
     * Secrets Management Code
     **************************************************************************/
    /*
        Secret is like this :
        
        {"username":"root","password":"password","engine":"mysql",
         "host":"demo-1.cluster-cwgymxni8kom.eu-west-1.rds.amazonaws.com",
         "port":3306,"dbClusterIdentifier":"demo-1"}
    */
    @SuppressWarnings("unchecked")
    public static Map<String,Object> getSecret(String secretName) {
    
        String region = "eu-west-1";
    
        // Create a Secrets Manager client
        AWSSecretsManager client  = AWSSecretsManagerClientBuilder.standard()
                                        .withRegion(region)
                                        .build();
        
        // In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
        // See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        // We rethrow the exception by default.
        
        String secret = "", decodedBinarySecret = "";
        GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest()
                        .withSecretId(secretName);
        GetSecretValueResult getSecretValueResult = null;
    
        try {
            getSecretValueResult = client.getSecretValue(getSecretValueRequest);
        } catch (DecryptionFailureException e) {
            // Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            // Deal with the exception here, and/or rethrow at your discretion.
            System.out.println(e);
            throw e;
        } catch (InternalServiceErrorException e) {
            // An error occurred on the server side.
            // Deal with the exception here, and/or rethrow at your discretion.
            System.out.println(e);
            throw e;
        } catch (InvalidParameterException e) {
            // You provided an invalid value for a parameter.
            // Deal with the exception here, and/or rethrow at your discretion.
            System.out.println(e);
            throw e;
        } catch (InvalidRequestException e) {
            // You provided a parameter value that is not valid for the current state of the resource.
            // Deal with the exception here, and/or rethrow at your discretion.
            System.out.println(e);
            throw e;
        } catch (ResourceNotFoundException e) {
            // We can't find the resource that you asked for.
            // Deal with the exception here, and/or rethrow at your discretion.
            System.out.println(e);
            throw e;
        }
    
        // Decrypts secret using the associated KMS CMK.
        // Depending on whether the secret is a string or binary, one of these fields will be populated.
        if (getSecretValueResult.getSecretString() != null) {
            secret = getSecretValueResult.getSecretString();
            // System.out.println(secret);
        }
        else {
            //this is a demo configuration error and should not happen
            throw new Error("Demo Secret must be plain text");
        }
    
        Map<String,Object> result = null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            result = mapper.readValue(secret, Map.class);
        } catch (IOException e) {
            System.out.println(e);

        }
        return result;
    }  
    
    /***************************************************************************
     * Exponential retry & backoff code
     **************************************************************************/
    
    //wait for 10 seconds max.
    private static final int MAX_WAIT_INTERVAL = 10000;
    private static final int MAX_RETRIES = 3;
    
    /*
     * Performs an synchornous operation, and implement an exponential backoff retry strategy
     */
    public static void doOperationAndRetry() {
    
        int retries = 0;
        boolean retry = true;
        try {

            // Do some synchronous operation.
            System.out.println("Going to query database");
            queryDatabase();
            
        } catch (Exception e) {
            
            //an exception occured 
            System.out.println(e);
            
            // retry !
            do {
                try {
                    // in real life, add jitter as per 
                    // https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
                    long waitTime = Math.min(getWaitTimeExp(retries), MAX_WAIT_INTERVAL);
                    System.out.println("Going to retry in " + waitTime + " msecs" );
        
                    // Wait for the retry.
                    Thread.sleep(waitTime);
    
                    // retry 
                    System.out.println("Going to retry to query database");
                    queryDatabase();
                    
                    // succeded 
                    retry = false;
                    
                } catch (Exception ex) {
                    // should change "retry" flag depending on error reported
                    System.out.println(ex);
                }
    
            } while (retry && (retries++ < MAX_RETRIES));
            
        }
    }
    
    /*
     * Returns the next wait interval, in milliseconds, using an exponential
     * backoff algorithm.
     */
    public static long getWaitTimeExp(int retryCount) {
    
        long waitTime = ((long) Math.pow(2, retryCount + 1) * 1000L);
    
        return waitTime;
    }
}