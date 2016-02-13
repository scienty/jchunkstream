package org.scienty.file.patch;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.scienty.buffer.ByteBufferFactory;

public class TrackingFileAssembler {
	public final static String PART_EXT = ".part";
	public final static String LOG_EXT = ".binlog";
	public final static int BUFFER_SIZE = 1024*1024;
	
	private FileAssembler assembler = null;
	private File file = null;
	private File partFile = null;
	private File logFile = null;
	private boolean asyncAssembly = false;
	private AssemblyLog assemblyLog;
	private boolean autoCleanupOnCompletion = true;
	private ByteBufferFactory buffFactory = null;

	public TrackingFileAssembler(File file, boolean async) {
		this(file, null, async);
	}

	public TrackingFileAssembler(File file, File logFile, boolean async) {
		this.file = file;
		this.partFile = new File(toPartFile(file.getPath()));
		if ( logFile == null ) logFile = new File(toLogFile(file.getPath()));
		this.logFile = logFile;
		this.asyncAssembly = async;
	}
	
	public void init(long targetSize, String tag) throws IOException, SignatureException {
		if ( assembler != null ) {
			throw new IllegalStateException("This method should be called only once for the object");
		}
		
		if ( file.exists() ) {
			throw new IllegalStateException("File already exist" + file.getPath());
		} else if (logFile.exists() == false && partFile.exists()) {
			throw new IllegalStateException("Missing tracker file" + logFile.getPath());
		} else if ( logFile.exists() && partFile.exists() == false) {
			throw new IllegalStateException("Dangling tracker file" + logFile.getPath());
		}  else {
			File folder = file.getParentFile();
			if ( folder.exists() && folder.isDirectory() == false) {
				throw new IllegalArgumentException("File is not a folder " + folder.getPath());
			} else if ( folder.exists() == false && folder.mkdirs() == false ) {
				throw new FileNotFoundException("Failed to create directory " + folder.getPath());
			}
		}
		
		if ( assemblyLog == null ) {
			assemblyLog = new AssemblyLog(logFile);
		} 

		assemblyLog.initFile(0L, targetSize-1, tag);
		
		AssemblyLogHeader head = assemblyLog.header();
		if (head.span().low != 0 || head.span().high != targetSize -1 ||
				(tag != null && tag.equals(head.tag()) == false) ) {
			throw new SignatureException ("Tag/Size missmatch");
		}
		
		if ( asyncAssembly )
			assembler = new AsyncFileAssembler(partFile, assemblyLog);
		else
			assembler = new SyncFileAssembler(partFile, assemblyLog);
		
		assembler.init();
		buffFactory = new ByteBufferFactory(BUFFER_SIZE);
	}
	
	public ChunkOutputStream getOutputStream(long startOffset, long endOffset) {
		return new ChunkOutputStream(assembler, startOffset, endOffset, buffFactory);
	}
	
	public void write(ByteBuffer buff, long startPos) {
		assembler.write(buff, startPos);
	}
	
	public long consume(InputStream input, long startPos, long endPos) throws IOException {
		//TODO: optimize this using buffer pool
		
		long totalToread = endPos - startPos + 1;
		long remToread = totalToread;
		
		while ( true ) {
			int toread = (remToread > BUFFER_SIZE) ? BUFFER_SIZE : (int)remToread; 
			byte[] bytes = new byte[BUFFER_SIZE];
			ByteBuffer buff = ByteBuffer.wrap(bytes);
			
			int index =0;
			int read = 0;
			while (index<toread) {
				read = input.read(bytes,index,toread-index);
				if ( read == -1 ) {
					break;
				}
				index+=read;
			}
			
			if (index > 0) {
				//something is there in buffer
				//System.out.println("w " + startPos + "-" + (startPos+index-1));
				buff.clear(); buff.limit(index);
				assembler.write(buff, startPos);
				
				startPos +=index;
				remToread -= index;
			}
			if ( remToread <= 0 || read == -1) break;
		}
		
		return totalToread - remToread;
	}
	
	public void close() throws IOException {
		if ( assembler != null ) {
			assembler.close();
		}
		
		if ( assemblyLog != null ) {
			if ( autoCleanupOnCompletion ) {
				boolean complete = false;
				if ( assemblyLog.read().size() == 0 ) {
					complete = true;
				}
				assemblyLog.close();
				
				if (complete) {
					assemblyLog.delete();
					partFile.renameTo(file);
				}
			} else {
				assemblyLog.close();
			}
			
		}
	}
	
	public AssemblyLog assemblyLog() {
		if ( assemblyLog == null ) throw new IllegalStateException("Not initialized");
		return assemblyLog;
	}
	
	public static String toPartFile(String path) {
		return path + PART_EXT;
	}
	
	public static String toLogFile(String path) {
		return path + LOG_EXT;
	}

	public static void main (String args[]) throws IOException, SignatureException {
		File file = new File("C:/temp1/cubparts.jpg");
		File outFile = new File("C:/temp1/order1/cubparts.jpg");
		outFile.delete();
		FileInputStream input = new FileInputStream(file);
		TrackingFileAssembler assember = new TrackingFileAssembler(outFile, true);
		assember.init(file.length(), "abc");
		System.out.println(assember.assemblyLog().read().getRanges());
		long count = assember.consume(input, 0, file.length()/2);
		System.out.println(assember.assemblyLog().read().getRanges());
		count += assember.consume(input, file.length()/2 + 1, file.length() -1);
		System.out.println(assember.assemblyLog().read().getRanges());
		assember.close();
		System.out.println("Count " + count);
	}
}
