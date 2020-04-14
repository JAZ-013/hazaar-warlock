# Services

Services are long running processes that allow code to be executed on the server in the background without affecting or requiring any interaction with the front-end. Services are managed by the Warlock process and can be set to start when Warlock starts or enabled/disabled manually using the Hazaar\Warlock\Control class.

Services are executed within the Application context and therefore have access to everything (configs, classes/models, cache, etc) that your application front-end does.

Services also honour the APPLICATION_ENV setting so that you can have a service defined and enabled in your development environment, but not in your production environment. This is handy as application environments are arbitrary and can be defined as anything you want. As such they are handy for altering the way an application functions depending on the server it is located by using the APPLICATION_ENV constant and the server's configuration.

Here are some very basic examples of how you could possibly use a service:

* Periodically checking a database for new records and then performing some action. For example, you could checkout an email outbox every minute and then send emails in bulk. This would prevent the front-end from hanging while processing emails.
* Monitoring a system service and then performing some action. For example, you could monitor the server memory usage and send an email (or trigger an event) when a threshold is reached.
* As an event driven event logger. You could get the service to subscribe to a chat event then when an event is triggered the service writes a record to a database.

But really, it's up to you as the developer as to how you use them and the possibilities are endless.

## Configuring a Service

To define and configure a service you need to create a file names service.ini that lives in your application config directory. The format of this file is the same as all other application config files and is defined as:

```json
{
    "application_env": {
        "service_name":{
            "setting": "value"
        }
    }
}
```

In the context of services, a service is defined as key/value pairs with the parent key the name of the service. For example, to define a service called *myTest* you would add the following to the appropriate application environment section of the *service.ini* file.

```json
{
    "development": {
        "myTest":{
            "enabled": true
        }
    }
}
```

The above is the absolute minimum required to define a service configuration. This service configuration is accessible from within the service class local member ```$this->options```.

## Writing the Service

Services are stored in the APPLICATION_BASE/service directory and extend the abstract class Hazaar\Warlock\Service. The class does not actually need to define any methods at all at a minimum. This is because the service may use a 'mainloop' execution method or it may be event driven.

There are three optional methods that can be defined called init(), run() and shutdown().

* init() is executed once only when the service is first started
* run() is executed continuously during the execution lifecycle. This is also known as the service 'mainloop' method and MUST call the $this->sleep($interval) method at some point to prevent CPU hogging.
* shutdown() is executed once only when the service is being asked to exit gracefully.

### Example Minimal Service

A minimal service would be defined in the file APPLICATION_BASE/service/myTest.php and would look like:

```php
class myTestService extends \Hazaar\Warlock\Service {

    public function run(){

        $this->sleep(1);

    }

}
```

This service does absolutely nothing except loop endlessly until the service is disabled. The call to $this->sleep(1) is required to prevent the service from chewing up 100% of the CPU.

!!! danger

DO NOT use the standard PHP *sleep()* function call. Calling this may appear to work and will prevent the CPU from being hogged, but it blocks the service from receiving signals during the sleep. Using *$this->sleep()* allows the Warlock server and the service to continue to communicate during the sleep. This also means that it is safe to do things like $this->sleep(300) and still have a responsive service that reacts to event triggers.

## Services and Signals

Services have built-in functionality for communicating with the Warlock server to manage subscriptions and trigger events. This is done using built-in methods provided by the Hazaar\Warlock\Service class.

```php
Hazaar\Warlock\Service::subscribe($event_id, $callback)
```

The subscribe method is called to request a subscription for an event. When the event is triggered a function will be called with the event payload as an argument, just like a normal Warlock event trigger.

Two arguments are required. $event_id is the name of the event your service is subscribing to and $callback is the name of a public/protected method defined in your service class.

### Example

```php
class myTestService extends \Hazaar\Warlock\Service {

    public function init() {

        $this->subscribe('testevent', 'onEvent');

    }

    public function onEvent($event) {

        //Do something with $event here

    }

}
```

#### Hazaar\Warlock\Service::unsubscribe($event_id)

The unsubscribe method does the complete opposite and removes the service from the subscription queue so that it no longer receives events of ```$event_id```.

#### Hazaar\Warlock\Service::trigger($event_id, $payload)

To trigger an event you simply need to call the trigger method with the name of the event to be triggered, along with any extra data you want to send with the trigger as the $payload argument.

##### Example

This service simply triggers the 'testevent' event every 60 seonds with the text 

```
The time is xx:xx:xx
```

Subscribing to an event is not required in order to send triggers for that event.

```php
class myTestService extends \Hazaar\Warlock\Service {

    public function init() {

        $this->trigger('testevent', 'The time is ' . date('H:i:s'));

        $this->sleep(60);

    }

}
```

### Scheduling method execution

Inside a service, it is possible to schedule the execution of a method at a later time. This is normally what a service would be doing anyway but we have implemented functions to make this much simpler for you to do so that you don't have to mess around with dates and times. You can simply call a method that says when you want to run it, or how often, and the Service class will take care of it.

#### Hazaar\Warlock\Service::delay($timeout, $method)

This simply executes a method after _$timeout seconds.

##### Example

```php
$this->delay(60, 'myTestMethod');
```

Executes the myTestMethod() method in 60 seconds.

#### Hazaar\Warlock\Service::schedule($when, $method)

This simply executes a method at a specified date/time. The $when parameter is converted to a \Hazaar\Date object internally and used to calculate the execution time based on the current timezone. $when can be in any format supported byHazaar\Date.

##### Example

```php
$this->schedule('tomorrow 3am', 'myTestMethod');
```

Executes the myTestMethod() method at 3am the following day.

#### Hazaar\Warlock\Service::interval($interval, $method)

This allows a method to be executed regularly at a specified interval.

##### Example

```php
$this->interval(300, 'myTestMethod');
```

Executes the myTestMethod() method every 5 minutes.

#### Hazaar\Warlock\Service::cron($schedule, $method)

For more complex schedules, it possible to specify the schedule in CRON format. Wikipedia has a good description of the cron schedule format.

##### Example

```php
$this->cron('0,30 9-17 * * 1-5', 'myTestMethod');
```

Executes the myTestMethod() method at the 0th and 30th minute of hours between 9am and 5pm on every day from Monday to Friday.

## A Complete Example Service

This service is a bit more complex and will subscribe to events called 'testevent' which once received, it will convert to uppercase and then send in a new trigger. It will also send a trigger every 60 seconds with the current time. When the service is disabled it will also send a 'Goodbye!'.

### Service that sends and receives signals

```php
class myTestService extends \Hazaar\Warlock\Service {

    public function init(){

        $this->subscribe('testevent', 'onTestEvent');

    }

    public function run(){

        /*
         * Do a whole bunch of cool stuff here!!!
         */

        $this->trigger('testevent', 'The time is ' . date('H:i:s'));

        $this->sleep(60);

    }

    public function shutdown(){

        $this->trigger('testevent', 'Goodbye!');

    }

    public function onTestEvent($event){

        $this->trigger('testevent', strtoupper($event['data']));

    }

}
```

!!! notice

You don't have to subscribe or trigger events from your service. A service is just a normal program that automatically loops endlessly until it is terminated. You can use it to periodically check a database and then do some action, or monitor some system function, or anything else really. It's up to you!

### Service that checks a database and sends an email

```php
class myDatabaseService extends \Hazaar\Warlock\Service {

    private $db;

    public function init(){

        $this->db = new \Hazaar\DBI();

    }

    public function run(){

        $cursor = $this->db->outbox->find(array('sent' => false));

        if($cursor->count() > 0){

            while($email = $cursor->row()){

                $mailer = new \Hazaar\Mail();

                $mailer->setFrom($email['from_addr'], $email['from_name']);

                $mailer->addTo($email['to_addr'], $email['to_name']);

                $mailer->setSubject($email['subject']);

                $mailer->setBodyText($email['body']);

                if($mailer->send() == true){

                    $this->db->outbox->update(array('id' => $email['id']), array('sent' => true));

                }

            }

        }

        $this->sleep(60);

    }

}
```
