public class ControllerObject1{ //SHOULD IT BE PRIVATE?

    private Controller controller1 = null;
    // used for preserving Controller Object
    public ControllerObject1(Controller c){
        this.controller1 = c;
    }

    public Controller getObject(){
        return this.controller1;
    }
}