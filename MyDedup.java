import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MyDedup{

    public static class ChunkIndex implements Serializable{ //struc to record the info about chunk
        public String containerID;  //container that the chunk is located at
        public int offset;          //offset of starting point of chunk
        public int chunkSize;       //size of chunk

        public ChunkIndex() {
            this.containerID = new String();
            this.offset = 0;
            this.chunkSize = 0;
        }
    }

    public static class FileRecipes implements Serializable {
        public HashMap <String, List<ChunkIndex>> recipe; //(filename, list of chunks)

        public FileRecipes() {
            this.recipe = new HashMap<String, List<ChunkIndex>>();
        }
    }

    public static class IndexFile implements Serializable { 
        public int num_files;
        public long logical_chunks;
        public long unique_chunks;
        public long logical_bytes;
        public long unique_bytes;
        public int num_containers;
        public HashMap<String, ChunkIndex> index; // (checksum, chunkIndex)
        
        public IndexFile(){
            this.num_files = 0;
            this.logical_chunks = 0L;
            this.unique_chunks = 0L;
            this.logical_bytes = 0L;
            this.unique_bytes = 0L;
            this.num_containers = 0;

            this.index = new HashMap<String, ChunkIndex>();
        }
    }
    
    public static String sha1(byte[] input) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] hashbyte = md.digest(input);
        BigInteger i = new BigInteger(1, hashbyte);
        return i.toString(16);
    }

    public static int dMod(int base, int exp, int q) {
        int result = base % q;
        while (exp-1 > 0){
            result = result * (base%q);
            exp--;
        }
        return result % q;
    }

    public static List<Integer> computeBoundaries(byte[] input, int m, int d, int q, int max) { 
        //m: min_chunk (window size), d: multiplier, q: avg_chunk (modulo), max: max_chunk

        //sanity checks
        if (m <= 0){
            System.out.println("Error: min_chunk must be larger than 0");
            System.exit(1);
        }
        if (d <= 0){
            System.out.println("Error: d must be larger than 0");
            System.exit(1);
        }
        if (q <= 0){
            System.out.println("Error: avg_chunk must be larger than 0");
            System.exit(1);
        }
        if (max <= 0){
            System.out.println("Error: max_chunk must be larger than 0");
            System.exit(1);
        }
        if (max < m) {
            System.out.println("Error: max_chunk cannot be smaller than min_chunk");
            System.exit(1);
        }

        List<Integer> boundaries = new ArrayList<Integer>(); //boundary = starting position of chunk
        boundaries.add(0); //create first boundary at index 0

        int dm1 = dMod(d, m-1, q); //precompute d^m-1
        int prev = 0;

        for(int s=0; s+m <= input.length; s++){
            //create boundary if reaches max_chunk limit
            if ((s - boundaries.get(boundaries.size()-1) + 1) >= max) {
                boundaries.add(s+1);
                continue;
            }

            //calculate rfp
            int rfp = 0;
            if (s==0 || s == boundaries.get(boundaries.size()-1)) {
                for(int i=0; i<m; i++){
                    rfp += (input[s+i] * dMod(d, m-i, q)) % q;
                }
            }
            else {
                rfp = d * (prev - dm1 * input[s-1]) + input[s+m-1];
            }
            rfp = rfp % q;
            prev = rfp;

            //create boundary
            if ((rfp & (q-1)) == 0 && s+m < input.length){
                boundaries.add(s+m);
                s = s + m - 1;
            }
        }

        return boundaries;
    }
    
    public static void main(String[] args) throws IOException, FileNotFoundException, ClassNotFoundException, NoSuchAlgorithmException{

        switch(args[0]) {

            case "upload":
                if (args.length < 6){
                    System.out.println("Not enough arguments.");
                    System.exit(1);
                }
                else {
                    int m = Integer.parseInt(args[1]);
                    int q = Integer.parseInt(args[2]);
                    int max = Integer.parseInt(args[3]);
                    int d = Integer.parseInt(args[4]);

                    File f = new File(args[5]);
                    FileInputStream file_to_upload = new FileInputStream(f);
                    byte[] data = new byte[(int)f.length()];
                    file_to_upload.read(data);
                    file_to_upload.close();

                    //separate files into chunks
                    List<Integer> boundaries = computeBoundaries(data, m, d, q, max);

                    int num_files;
                    long logical_chunks;
                    long unique_chunks;
                    long logical_bytes;
                    long unique_bytes;
                    int num_containers;
                    
                    //load index
                    IndexFile indexFile;
                    File f_index = new File("metadata/mydedup.index");
                    if (!f_index.exists()){
                        f_index.getParentFile().mkdirs();
                        f_index.createNewFile();
                        indexFile = new IndexFile();
                        num_files = 0;
                        logical_chunks = 0;
                        unique_chunks = 0;
                        logical_bytes = 0;
                        unique_bytes = 0;
                        num_containers = 0;
                    }
                    else{
                        FileInputStream fin_index = new FileInputStream(f_index);
                        ObjectInputStream oin_index = new ObjectInputStream(fin_index);
                        indexFile = (IndexFile) oin_index.readObject();
                        oin_index.close();
                        num_files = indexFile.num_files;
                        logical_chunks = indexFile.logical_chunks;
                        unique_chunks = indexFile.unique_chunks;
                        logical_bytes = indexFile.logical_bytes;
                        unique_bytes = indexFile.unique_bytes;
                        num_containers = indexFile.num_containers;
                    }
                    
                    //load file recipes
                    FileRecipes fileRecipes;
                    File f_recipes = new File ("metadata/filerecipes.index");
                    if (!f_recipes.exists()){
                        f_recipes.getParentFile().mkdirs();
                        f_recipes.createNewFile();
                        fileRecipes = new FileRecipes();
                    }
                    else{
                        FileInputStream fin_recipes = new FileInputStream(f_recipes);
                        ObjectInputStream oin_recipes = new ObjectInputStream(fin_recipes);
                        fileRecipes = (FileRecipes) oin_recipes.readObject();
                        oin_recipes.close();
                    }

                    //prepare container
                    File dir = new File("data/");
                    if (!dir.exists()){
                        dir.mkdirs();
                    }
                    File containerFile = new File("data/"+(num_containers+1));
                    containerFile.createNewFile();
                    FileOutputStream container_out = new FileOutputStream(containerFile);
                    ByteArrayOutputStream container = new ByteArrayOutputStream();
                    int currentContainerBytes = 0;

                    List<ChunkIndex> chunkList = new ArrayList<ChunkIndex>();

                    for(int i=0; i<boundaries.size(); i++) {

                        byte[] currentChunk = Arrays.copyOfRange(data, boundaries.get(i), (i == boundaries.size()-1) ? data.length : boundaries.get(i+1));
                        String hash = sha1(currentChunk);
                        
                        if (!indexFile.index.containsKey(hash)){ //unique chunk

                            if (currentContainerBytes + currentChunk.length > 1048576){
                                container.writeTo(container_out);
                                container.reset();
                                currentContainerBytes = 0;
                                num_containers++;
                                containerFile = new File("data/"+(num_containers+1));
                                containerFile.createNewFile();
                                container_out = new FileOutputStream(containerFile);
                            }
                            ChunkIndex currentChunkIndex = new ChunkIndex();
                            currentChunkIndex.containerID = containerFile.getName();
                            currentChunkIndex.offset = currentContainerBytes;
                            currentChunkIndex.chunkSize = currentChunk.length;
                            container.write(currentChunk);
                            chunkList.add(currentChunkIndex);
                            indexFile.index.put(hash, currentChunkIndex);
                            currentContainerBytes += currentChunk.length;
                            unique_chunks += 1L;
                            unique_bytes += (long) currentChunk.length;
                            
                        }
                        else {
                            ChunkIndex currentChunkIndex = indexFile.index.get(hash);
                            chunkList.add(currentChunkIndex);
                        }

                        if (i == boundaries.size() - 1) { //tail container
                            container.writeTo(container_out);
                            num_containers++;
                        }

                        logical_chunks += 1L;
                        logical_bytes += (long) currentChunk.length;
                    }
                    container_out.close();

                    fileRecipes.recipe.put(args[5], chunkList);
                    num_files++;

                    //update stat in index file
                    indexFile.num_files = num_files;
                    indexFile.logical_chunks = logical_chunks;
                    indexFile.unique_chunks = unique_chunks;
                    indexFile.logical_bytes = logical_bytes;
                    indexFile.unique_bytes = unique_bytes;
                    indexFile.num_containers = num_containers;

                    //update index and file recipe
                    FileOutputStream fout_index = new FileOutputStream(f_index, false);
                    ObjectOutputStream oout_index = new ObjectOutputStream(fout_index);
                    oout_index.writeObject(indexFile);
                    oout_index.close();

                    FileOutputStream fout_recipes = new FileOutputStream(f_recipes, false);
                    ObjectOutputStream oout_recipes = new ObjectOutputStream(fout_recipes);
                    oout_recipes.writeObject(fileRecipes);
                    oout_recipes.close();

                    //report statistics
                    System.out.println("Total number of files that have been stored: " + num_files);
                    System.out.println("Total number of pre-deduplicated chunks in storage: " + logical_chunks);
                    System.out.println("Total number of unique chunks in storage: " + unique_chunks);
                    System.out.println("Total number of bytes of pre-deduplicated chunks in storage: " + logical_bytes);
                    System.out.println("Total number of bytes of unique chunks in storage: " + unique_bytes);
                    System.out.println("Total number of containers in storage: " + num_containers);
                    System.out.println("Deduplication ratio: " + ((float) logical_bytes/unique_bytes));
                }
                break;
            
            case "download":
                if (args.length < 3){
                    System.out.println("Not enough arguments.");
                    System.exit(1);
                }
                else {
                    FileInputStream fin_recipe = new FileInputStream("metadata/filerecipes.index");
                    ObjectInputStream oin_recipe = new ObjectInputStream(fin_recipe);
                    FileRecipes fileRecipes = (FileRecipes)oin_recipe.readObject();
                    if (!fileRecipes.recipe.containsKey(args[1])) {
                        System.out.println("Error: \"" + args[1] + "\" does not exist");
                        System.exit(1);
                    }
                    List<ChunkIndex> chunkList = fileRecipes.recipe.get(args[1]);
                    oin_recipe.close();

                    ByteArrayOutputStream data = new ByteArrayOutputStream();
                    for(int i=0; i<chunkList.size(); i++) {
                        ChunkIndex currentChunk = chunkList.get(i);
                        FileInputStream fin_container = new FileInputStream("data/"+currentChunk.containerID);
                        fin_container.skip(currentChunk.offset);
                        byte[] containerData = new byte[currentChunk.chunkSize];
                        fin_container.read(containerData);
                        data.write(containerData);
                        fin_container.close();
                    }

                    File fout = new File(args[2]);
                    if (fout.getParentFile() != null) {
                        fout.getParentFile().mkdirs();
                    }
                    if(!fout.exists()){
                        fout.createNewFile();
                    }
                    FileOutputStream newFile = new FileOutputStream(fout);
                    data.writeTo(newFile);
                    newFile.close();
                }
                break;
            
            // functions for testing
            // case "hash":
            //     File f = new File(args[1]);
            //     FileInputStream fin = new FileInputStream(f);
            //     byte[] arr = new byte[(int)f.length()];
            //     fin.read(arr);
            //     fin.close();

            //     System.out.println(sha1(arr));
            // break;

            // case "pow":
            //     int e = Integer.parseInt(args[1]);
            //     int p = Integer.parseInt(args[2]);
            //     double d = Math.pow(e, p);
            //     System.out.println(d);
            //     int x = (int)(d/Math.pow(1, 20));
            //     System.out.println(x);
            // break;

            // case "mod":
            //     int base = Integer.parseInt(args[1]);
            //     int exp = Integer.parseInt(args[2]);
            //     int q = Integer.parseInt(args[3]);
            // break;

            // case "chunking":
            //     // byte[] data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 
            //     //             11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            //     //             21, 22, 23, 24, 25, 26, 27, 28, 29, 30};
            //     File f = new File(args[5]);
            //     FileInputStream fin = new FileInputStream(f);
            //     byte[] data = new byte[(int)f.length()];
            //     fin.read(data);
            //     fin.close();
            //     int m = Integer.parseInt(args[1]);
            //     int q = Integer.parseInt(args[2]);
            //     int max = Integer.parseInt(args[3]);
            //     int d = Integer.parseInt(args[4]);

            //     List<Integer> boundaries = computeBoundaries(data, m, d, q, max);
            //     for (int n: boundaries){
            //         System.out.println(n);
            //     }
            //     System.out.println("data (length" + data.length + ") : " );
            //     int count = 0;
            //     for(byte b: data){
            //         System.out.println(count + ": " + (char)b);
            //         count++;
            //     }
            //     System.out.println(" ");

            //     for(int i=0; i<boundaries.size(); i++) {
            //         byte[] arr;
            //         if (i == boundaries.size() - 1){
            //             arr = Arrays.copyOfRange(data, boundaries.get(i), data.length);
            //         } else {
            //             arr = Arrays.copyOfRange(data, boundaries.get(i), boundaries.get(i+1));
            //         }
            //         System.out.println("chunk" + i);
            //         for(byte b: arr) {
            //             System.out.println((char)b);
            //         }
            //         System.out.println(" ");
            //     }
            // break;

            default:
                System.out.println("Unknown command \"" + args[0] + "\"");
                System.exit(1);
        }
    }
}