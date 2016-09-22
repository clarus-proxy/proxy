package eu.clarussecure.dataoperations.anonymization;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

public class SymmetricCrypto {

       public static final String KEY = "."+File.separator+"keys"+File.separator+"AES.key";

       public static byte[] encrypt(String text, String key) throws FileNotFoundException, IOException, ClassNotFoundException {
           return encrypt(text.getBytes(), key);
       }
       public static byte[] encrypt(byte[] text, String key) throws FileNotFoundException, IOException, ClassNotFoundException {
           ObjectInputStream keyStream = new ObjectInputStream(new FileInputStream(key));
           final SecretKey publicKey = (SecretKey) keyStream.readObject(); //la key no ha de canviar per a res, aixi que la definim com a final
           keyStream.close();
           return encrypt(text, publicKey);
       }
       public static byte[] encrypt(byte[] text, SecretKey key) {
         byte[] cipherText = null;

         try {
           final Cipher cipher = Cipher.getInstance("AES");
           cipher.init(Cipher.ENCRYPT_MODE, key);
           cipherText = cipher.doFinal(text);
         } catch (Exception e) {
           e.printStackTrace();
         }
         return cipherText;
       }


       public static byte[] decrypt(String text, String key) throws FileNotFoundException, IOException, ClassNotFoundException {
           return decrypt(text.getBytes(), key);
       }
       public static byte[] decrypt(byte[] text, String key) throws FileNotFoundException, IOException, ClassNotFoundException {
           ObjectInputStream keyStream = new ObjectInputStream(new FileInputStream(key));
           final SecretKey publicKey = (SecretKey) keyStream.readObject();
           keyStream.close();
           return decrypt(text, publicKey);
       }
       public static byte[] decrypt(byte[] text, SecretKey key) {
         byte[] dectyptedText = null;
         try {
           final Cipher cipher = Cipher.getInstance("AES");
           cipher.init(Cipher.DECRYPT_MODE, key);
           dectyptedText = cipher.doFinal(text);
         } catch (Exception ex) {
           ex.printStackTrace();
         }

         return dectyptedText;
       }

       public static void generateKey() {
             try {
               final KeyGenerator keyGen = KeyGenerator.getInstance("AES");
               keyGen.init(128);
               final SecretKey key = keyGen.generateKey();

               File keyFile = new File(KEY);

               // Create files to store public and private key
               if (keyFile.getParentFile() != null) {
                 keyFile.getParentFile().mkdirs();
               }
               keyFile.createNewFile();

               // Saving the Private key in a file
               ObjectOutputStream keyOS = new ObjectOutputStream(
                   new FileOutputStream(keyFile));

               keyOS.writeObject(key);
               keyOS.close();
             } catch (Exception e) {
               e.printStackTrace();
             }

           }
}

