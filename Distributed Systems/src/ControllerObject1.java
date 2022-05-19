public class ControllerObject1{ //SHOULD IT BE PRIVATE?

    private Controller1 controller1 = null;
    //preserves a single object of the controller class instead of creating multiple instances of it
    public ControllerObject1(Controller1 c){
        this.controller1 = c;
    }

    public Controller1 getObject(){
        return this.controller1;
    }
}