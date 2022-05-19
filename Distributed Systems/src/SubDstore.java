public class SubDstore {
    private Dstore dstoreObject = null;
    //preserves a single object of the controller class instead of creating multiple instances of it
    public SubDstore(Dstore d){
        this.dstoreObject = d;
    }

    public Dstore getObject(){
        return this.dstoreObject;
    }
}
