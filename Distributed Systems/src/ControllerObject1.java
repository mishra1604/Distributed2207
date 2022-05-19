public class ControllerObject1{ //SHOULD IT BE PRIVATE?

    private Controller controller1 = null;
    //preserves a single object of the controller class instead of creating multiple instances of it
    public ControllerObject1(Controller c){
        this.controller1 = c;
    }

    public Controller getObject(){
        return this.controller1;
    }
}