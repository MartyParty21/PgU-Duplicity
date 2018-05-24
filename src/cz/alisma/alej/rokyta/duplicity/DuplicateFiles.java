package cz.alisma.alej.rokyta.duplicity;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
 
public class DuplicateFiles {
    public static void main(String[] args) {
        // Mapovani mezi hashem (MD5) a soubory se stejnym hashem.
        Map<String, List<File>> files = new HashMap<>();
 
        // Spocitame hashe pro kazdy soubor (nebo rekurzivne pro adresare).
        for (String filename : args) {
            computeHashes(new File(filename), files);
        }
 
        // Projdeme mapovani a pokud je vice souboru se stejnym hashem,
        // porovname je bajt po bajtu.
        for (Map.Entry<String, List<File>> entry : files.entrySet()) {
            File[] similar = entry.getValue().toArray(new File[0]);
 
            if (similar.length == 1) {
                continue;
            }
 
            for (int i = 0; i < similar.length - 1; i++) {
                for (int j = i + 1; j < similar.length; j++) {
                    try {
                        // Stejna cesta?
                        if (similar[i].getCanonicalPath().equals(similar[j].getCanonicalPath())) {
                            continue;
                        }
 
                        // Porovname bajt po bajtu
                        if (isSameFile(similar[i], similar[j])) {
                            System.out.printf("%s and %s have the same content.\n", similar[i], similar[j]);
                        }
                    } catch (IOException e) {
                    }
                }
            }
        }
    }
 
    private static void computeHashes(File file, Map<String, List<File>> hashes) {
        if (file.isFile()) {
            // Pro bezny soubor spocitame hash.
            // Pri chybe budeme soubor ignorovat (neni nejlepsi reseni,
            // ale tady bude fungovat rozumne).
            String hash;
            try {
                hash = getFileHash(file);
            } catch (IOException e) {
                return;
            } catch (NoSuchAlgorithmException e2) {
                return;
            }
            List<File> similar = hashes.get(hash);
            if (similar == null) {
                similar = new ArrayList<>();
                hashes.put(hash, similar);
            }
            similar.add(file);
        } else if (file.isDirectory()) {
            // Pro adresare projdeme vsechny soubory v nem.
            File[] files = file.listFiles();
            for (File f : files) {
                computeHashes(f, hashes);
            }
        }
    }
 
 
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
 
 
    private static boolean isSameFile(File af, File bf) throws IOException {
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
 
 
    private static void DEBUG(String fmt, Object... args) {
        System.out.printf("[debug]: " + fmt + "\n", args);
    }
}