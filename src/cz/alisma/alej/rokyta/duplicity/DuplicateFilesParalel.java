package cz.alisma.alej.rokyta.duplicity;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DuplicateFilesParalel {
	// Mapovani mezi hashem (MD5) a soubory se stejnym hashem.
    public static Map<String, List<File>> files = new HashMap<>();
	public static void main(String args[]) throws InterruptedException, ExecutionException{
		// Mapovani mezi hashem (MD5) a soubory se stejnym hashem.
	    Map<String, List<File>> files = new HashMap<>();
		ExecutorService executor = Executors.newFixedThreadPool(4);
		
		for(String arg : args)
		{
			createFutures(new File(arg), files, executor);
		}
		
		List<Future<ComparedFiles>> comparisons = new ArrayList<>();
		// Projdeme mapovani a pokud je vice souboru se stejnym hashem,
        // porovname je bajt po bajtu.
        for (Map.Entry<String, List<File>> entry : files.entrySet()) {
            File[] similar = entry.getValue().toArray(new File[0]);
 
            if (similar.length == 1) {
                continue;
            }
 
            for (int i = 0; i < similar.length - 1; i++) {
                for (int j = i + 1; j < similar.length; j++) {
                	Comparator comparator = new Comparator(similar[i], similar[j]);
                	Future<ComparedFiles> compFiles = executor.submit(comparator);
                	comparisons.add(compFiles);
                }
            }
        }	
        executor.shutdown();
        
        for (Future<ComparedFiles> comp : comparisons) {
            ComparedFiles result = comp.get();
            DEBUG("%s %s the same content as %s", result.getAf().getName(), result.getSimilarity() ? "has" : "doesnt have", result.getBf().getName());
        }
	}

	private static void createFutures(File file, Map<String, List<File>> files, ExecutorService executor)
	{	
		if(file.isFile())
		{
			// ziskani hashe a prime zarazeni do mapy
			// Autorem zakladu kodu V. Horky
			String hash = null;
            try {
                hash = getHash(file, executor).get().getHash();
            } catch (InterruptedException e) {
			} catch (ExecutionException e) {
			}
            List<File> similar = files.get(hash);
            if (similar == null) {
                similar = new ArrayList<>();
                files.put(hash, similar);
            }
            similar.add(file);
		}
		else if(file.isDirectory())
		{
			File[] subFiles = file.listFiles();
            for (File f : subFiles) {
            	createFutures(f, files, executor);
            }
		}
	}
	
	public static Future<FileHash> getHash(File file, ExecutorService executor)
	{
		// vytvoreni futuru
		HashComputer hashComputer = new HashComputer(file);
        Future<FileHash> hash = executor.submit(hashComputer);
        return hash;
	}
	
	public static class HashComputer implements Callable<FileHash> {
		private final File file;

		public HashComputer(File file) {
			this.file = file;
		}
		
		// Prevzato z puvodniho programu
		private static String getFileHash(File f) throws IOException, NoSuchAlgorithmException {
	        MessageDigest digest = MessageDigest.getInstance("MD5");
	 
	        InputStream is = new FileInputStream(f);
	 
	        byte[] data = new byte[1024];
	 
	        while (true) {
	            int actuallyRead = is.read(data);
	            if (actuallyRead == -1) {
	                break;
	            }
	            digest.update(data, 0, actuallyRead);
	        }
	 
	        is.close();
	 
	        byte[] digestBytes = digest.digest();
	 
	        StringBuffer sb = new StringBuffer();
	        for (byte b : digestBytes) {
	            sb.append(String.format("%02x", b));
	        }
	 
	        String hash = sb.toString();
	 
	        DEBUG("%s => %s", f.getName(), hash);
	 
	        return hash;
	    }

		@Override
		public FileHash call() throws Exception {
			return new FileHash(getFileHash(file), file); 
		}		
	}
	
	private static class FileHash {		
		private final String hash;
		
		public FileHash(String hash, File file) {
			this.hash = hash;
		}

		public String getHash() {
			return hash;
		}
	}
	
	public static class Comparator implements Callable<ComparedFiles> {
		private final File af;
		private final File bf;
		
		public Comparator(File similar, File similar2) {
			this.af = similar;
			this.bf = similar2;
		}

		private static boolean isSameFile(File af, File bf) throws IOException {
			try {
			// Stejna cesta?
				if (af.getCanonicalPath().equals(bf.getCanonicalPath())) {
					return true;
				}
			}catch (IOException e) {
			}
			
			DEBUG("Comparing %s and %s.", af, bf);
			 
	        InputStream a = new FileInputStream(af);
	        InputStream b = new FileInputStream(bf);
	 
	        try {
	            while (true) {
	                int aByte = a.read();
	                int bByte = b.read();
	                if ((aByte == -1) && (bByte == -1)) {
	                    return true;
	                }
	                if (aByte != bByte) {
	                    return false;
	                }
	            }
	        } finally {
	            a.close();
	            b.close();
	        }
		}
		
		@Override
		public ComparedFiles call() throws Exception {
			return new ComparedFiles(af, bf, isSameFile(af, bf));
		}	
	}
	
	private static class ComparedFiles {
		private final File af;
		private final File bf;
		private final boolean isSameFile;
		
		public ComparedFiles(File af, File bf, boolean isSameFile) {
			this.af = af;
			this.bf = bf;
			this.isSameFile = isSameFile;
		}
		
		public File getAf()
		{
			return af;
		}
		
		public File getBf()
		{
			return bf;
		}
		
		public boolean getSimilarity()
		{
			return isSameFile;
		}
		
	}
	
    private static void DEBUG(String fmt, Object... args) {
        System.out.printf("[debug]: " + fmt + "\n", args);
    }
}
