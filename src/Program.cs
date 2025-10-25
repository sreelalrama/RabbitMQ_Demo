using RabbitMQ_Demo;

var simpleQueueDemo = new SimpleQueueDemo();
var workQueueDemo = new WorkQueueDemo();
var fanOutQueuesDemo = new FanoutQueuesDemo();
var routing_DirectExchangeDemo = new Routing_DirectExchangeDemo();
var topicExchangeDemo = new TopicExchangeDemo();
var jsonMessageDemo = new JsonMessageDemo();
var rPC_PatternDemo = new RPC_PatternDemo();
//simpleQueueDemo.PerformDemo();
//workQueueDemo.PerformDemo();
//fanOutQueuesDemo.PerformDemo();
//routing_DirectExchangeDemo.PerformDemo();
//topicExchangeDemo.PerformDemo();
//jsonMessageDemo.PerformDemo();
rPC_PatternDemo.PerformDemo();

