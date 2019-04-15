# Global Events

Global events is a new feature of Warlock 2.2.  Previously, to handle an event, something had to subscribe to the event.  This was
originally intended to be used to allow clients to interact with each other, but with the adition of services, what became apparent
was that signalling a service to do something was also useful.

The downside to using a service is that the service must be running all the time for this to work.  If the service failed or shutdown
for some reason, then the event would go un-handled.  This also meant that resources would be consumed while the service is running 
even though the service isn't actually doing anything. 

With global events, the main Warlock process is technically the client subscribing to an event and it will start up a short-lived
event handler process to handle the event trigger.

!!! Warning

Services are still able to subscribe to events.  This is still very useful for some things that need a more immediate response.  Using global events introduces some delays while the event handler process is started that may be undesirable.


## Example Usage

The first thing to do is to define your event handler.  Because this is being executed in an empty application context (ie: without a
service) the method must be a static class method.

```php
namespace Application\Model;

class MyTestClass {

    static public function handleTestEvent($data, $payload){
    
        $this->log(W_INFO, 'Recieved: ' $data['message']);

    }

}
```

The next step is to tell Warlock what event this handler is for by setting up a subscription in the main warlock configuration file `warlock.json`.

```json
{
    "{{APPLICATION_ENV}}":{
        "subscribe": {
            "test": "Application\\Model\\myTestClass::handleTestEvent"
        }
    }
}
```

Where *APPLICATION_ENV* is the application environment you are configuring, of course.

Now, when a client triggers the **test** event, the `Application\\Model\\myTestClass::handleTestEvent` method will be execute and the
trigger data will be sent as the first argument.  This is consistent with existing event handlers.
