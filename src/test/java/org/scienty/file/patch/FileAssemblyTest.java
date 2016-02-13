/**
 * 
 */
package org.scienty.file.patch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * @author prakasid
 *
 */
public class FileAssemblyTest {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		
	    File tmpFile = File.createTempFile("prakash", "champa");
		tmpFile.getAbsolutePath();
		
		BufferedReader reader = new BufferedReader(new FileReader(tmpFile));
		char [] buff = new char[10];
		int read = reader.read(buff);
		while ( read > 0 ) {
			System.out.print(String.valueOf(buff,0, read));
			read = reader.read(buff);
		}
		reader.close();
		tmpFile.delete();
	}

}
