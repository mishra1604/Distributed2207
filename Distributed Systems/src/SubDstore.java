public class SubDstore {
    private Dstore dstoreObject = null;
    //used for getting the Object of the Dstore class
    public SubDstore(Dstore d){
        this.dstoreObject = d;
    }

    public Dstore getObject(){
        return this.dstoreObject;
    }
}
