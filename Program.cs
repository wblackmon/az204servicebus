

// See https://aka.ms/new-console-template for more information
using Azure.Messaging.ServiceBus;

Console.WriteLine("Hello, Service Bus!");

// --Exercise: Send and receive message from a Service Bus queue by using .NET.

// 1. Launch the Azure Cloud Shell and select Bash and the environment.
// 2. Create variables used in the Azure CLI commands. Replace <myLocation> with a region near you.

// myLocation=<myLocation>
// myNameSpaceName=az204svcbus$RANDOM

// --Create Azure resources
// 1. Create a resource group to hold the Azure resources you're creating.

// az group create --name az204-svcbus-rg --location $myLocation

// 2. Create a Service Bus messaging namespace. The following command creates a namespace using the variable you created earlier. The operation takes a few minutes to complete.

// az servicebus namespace create \
//     --resource-group az204-svcbus-rg \
//     --name $myNameSpaceName \
//     --location $myLocation

// az servicebus namespace create --resource-group az204-svcbus-rg --name $myNameSpaceName --location $myLocation

// 3. Create a Service Bus queue

// az servicebus queue create --resource-group az204-svcbus-rg \
//     --namespace-name $myNameSpaceName \
//     --name az204-queue

// az servicebus queue create --resource-group az204-svcbus-rg --namespace-name $myNameSpaceName --name az204-queue
// az servicebus queue show --resource-group az204-svcbus-rg --namespace-name az204svcbus24989 --name az204-queue


// --Retrieve the connection string for the Service Bus Namespace
// 1. Open the Azure portal and navigate to the az204-svcbus-rg resource group.

// 2. Select the az204svcbus resource you created.

// 3. Select Shared access policies in the Settings section, then select the RootManageSharedAccessKey policy.

// 4. Copy the Primary Connection String from the dialog box that opens up and save it to a file, or leave the portal open and copy the key when needed.

// 5. Retrieve the primary connection string using the Azure CLI:

// az servicebus namespace authorization-rule keys list --resource-group az204-svcbus-rg --namespace-name az204svcbus24989 --name RootManageSharedAccessKey --query primaryConnectionString -o tsv

// connection string to your Service Bus namespace
string connectionString = "SERVICE_BUS_CONNECTION_STRING";

// name of your Service Bus topic
string queueName = "az204-queue";

// the client that owns the connection and can be used to create senders and receivers
ServiceBusClient? client = null;

// the sender used to publish messages to the queue
ServiceBusSender? sender = null;

// Create a processor that we can use to process the messages
ServiceBusProcessor? processor = null;

try
{
    // create a Service Bus client
    client = new ServiceBusClient(connectionString);

    // create a sender for the queue
    sender = client.CreateSender(queueName);

    // create processor to process messages
    processor = client.CreateProcessor(queueName, new ServiceBusProcessorOptions());

    // create a message batch to send multiple messages to the queue
    using ServiceBusMessageBatch messageBatch = await sender.CreateMessageBatchAsync();

    for (int i = 0; i < 3; i++)
    {
        if (!messageBatch.TryAddMessage(new ServiceBusMessage($"Message {i}")))
        {

            throw new Exception($"Message {i} is too large to fit in the batch and cannot be sent.");
        }
    }
    // if the batch is full, send it and then create a new batch
    await sender.SendMessagesAsync(messageBatch);

    // Verify that the messages were sent to the queue
    ServiceBusReceiver receiver = client.CreateReceiver(queueName);
    Console.WriteLine("Receiving messages from the queue...");

    // Recieve messages from the queue
    int messagesReceived = 0;
    IReadOnlyList<ServiceBusReceivedMessage> receivedMessages = await receiver.ReceiveMessagesAsync(maxMessages: 3);
    foreach (ServiceBusReceivedMessage message in receivedMessages)
    {
        Console.WriteLine($"Received: {message.Body}");
        messagesReceived++;
    }
    Console.WriteLine($"Received {messagesReceived} messages from the queue.");

    // add handlers to process messages and any errors
    processor.ProcessMessageAsync += MessageHandler;
    processor.ProcessErrorAsync += ErrorHandler;

    await processor.StartProcessingAsync();

    Console.WriteLine("Wait for a minute and then press any key to end the processing");
    Console.ReadKey();

    // stop processing and close the processor
    Console.WriteLine("\nStopping the processor");
    await processor.StopProcessingAsync();
}
catch (Exception exception)
{
    Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
}
finally
{
    // dispose of the client object
    if (client != null)
    {
        await client.DisposeAsync();
    }
    if (sender != null)
    {
        await sender.DisposeAsync();
    }
    if (processor != null)
    {
        await processor.DisposeAsync();
    }
}

// Handle recieved messages
async Task MessageHandler(ProcessMessageEventArgs args)
{
    string body = args.Message.Body.ToString();
    Console.WriteLine($"Received: {body}");

    // complete the message. messages is deleted from the queue.
    await args.CompleteMessageAsync(args.Message);
}

// Handle any errors when receiving messages
Task ErrorHandler(ProcessErrorEventArgs args)
{
    Console.WriteLine(args.Exception.ToString());
    return Task.CompletedTask;
}


