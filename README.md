# rabbit-consuer

An application for testing Rabbitmq

## how to test

Step1: Clone repository

```
git clone https://github.com/vandungbkfet12/rabbit-consumer.git
```

Step2: Go to RabbitConsumer\bin\Release\netcoreapp3.1 and Edit configurations in apppsetting.json. Assume that you already have a installed rabbitmq server

```
{
  "HostName": "localhost",
  "Port":  5672,
  "QueueName": "test",
  "UserName": "guest",
  "Password": "guest",
  "CsvFileName": "ramp"
}
  ```
  Change configurations as you want.
  
  Step3: Run RabbitConsumer.exe and view results on console screens, exported csv file locates in the same RabbitConsumer.exe's folder


