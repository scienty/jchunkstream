package org.scienty.system.util;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.atomic.AtomicLong;

public class FileSystemUtil {

	static long getSize(String startPath) throws IOException {
	    final AtomicLong size = new AtomicLong(0);
	    Path path = Paths.get(startPath);

	    Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
	        @Override
	        public FileVisitResult visitFile(Path file,
	                BasicFileAttributes attrs) throws IOException {
	            size.addAndGet(attrs.size());
	            return FileVisitResult.CONTINUE;
	        }

	        @Override
	        public FileVisitResult visitFileFailed(Path file, IOException exc)
	                throws IOException {
	            // Skip folders that can't be traversed
	            System.out.println("skipped: " + file + "e=" + exc);
	            return FileVisitResult.CONTINUE;
	        }
	    });

	    return size.get();
	}
	
	public static String formatFileSize(long bytes) {
		return formatFileSize(bytes, false);
	}
	
	public static String formatFileSize(long bytes, boolean si) {
	    int unit = si ? 1000 : 1024;
	    if (bytes < unit) return bytes + " B";
	    int exp = (int) (Math.log(bytes) / Math.log(unit));
	    String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
	    return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
	}

}
