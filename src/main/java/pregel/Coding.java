package pregel;

public class Coding {

    public Coding(){ }

    public long encode(byte[] kmer) {
        long kmer_enc = 0;

        for (byte b : kmer) {
            kmer_enc <<= 2;
            switch (b) {
                case 'A':
                case 'a':
                    kmer_enc += 0; break;
                case 'C':
                case 'c':
                    kmer_enc += 1; break;
                case 'G':
                case 'g':
                    kmer_enc += 2; break;
                case 'T':
                case 't':
                    kmer_enc += 3; break;
            }
        }
        return kmer_enc;
    }

}
