using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Runtime;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace SqsRepro
{
    internal class Program
    {
        private const string EndpointName = "stability-repro-queue";
        private const int DefaultConnectionLimit = 150;
        private static readonly int concurrencyLevel = Environment.ProcessorCount * 4;
        private static readonly bool sending = true;

        private static readonly Random random = new Random();

        private static async Task Main(string[] args)
        {
            GCSettings.LatencyMode = GCLatencyMode.Batch;
            Console.Title = EndpointName + " | " + Environment.OSVersion + " | Is64BitProcess:" +
                            Environment.Is64BitProcess + " | LatencyMode:" + GCSettings.LatencyMode + " | IsServerGC:" +
                            GCSettings.IsServerGC;

            ServicePointManager.Expect100Continue = false;
            ServicePointManager.UseNagleAlgorithm = false;
            ServicePointManager.DefaultConnectionLimit =
                DefaultConnectionLimit; // Querying of DefaultConnectionLimit on dotnet core does not return assigned value

            Task ret;
            var client = new AmazonSQSClient(new AmazonSQSConfig
            {
                RegionEndpoint = RegionEndpoint.EUCentral1,
                /*ServiceURL =  "http://sqs.eu-central-1.amazonaws.com"*/
#if !NET461
                MaxConnectionsPerServer = 100, 
                /*CacheHttpClient = false,
                UseNagleAlgorithm = false*/
#endif
            });
            var queueUrl = await CreateQueue(client);

            var cancellationTokenSource = new CancellationTokenSource();
            var consumerTasks = new List<Task>();

            for (var i = 0; i < concurrencyLevel; i++)
                consumerTasks.Add(ConsumeMessage(client, queueUrl, i, cancellationTokenSource.Token));

            var sendingTask = Task.Run(async () =>
            {
                if (sending)
                {
                    var cts = new CancellationTokenSource();
                    cts.CancelAfter(TimeSpan.FromMinutes(5));
                    var sends = new List<Task>();

                    var sessionId = DateTime.UtcNow.ToString("s");
                    var batchSize = 100;
                    var count = 0;

                    while (!cts.Token.IsCancellationRequested)
                    {
                        var s = Stopwatch.StartNew();
                        sends.Clear();
                        for (var i = 0; i < batchSize; i++)
                        {
                            var id = $"{sessionId}/{++count:D8}";
                            sends.Add(RetryWithBackoff(
                                () => client.SendMessageAsync(new SendMessageRequest(queueUrl, $"{id}"), cts.Token),
                                cts.Token, id, 5));
                        }
                        try
                        {
                            await Task.WhenAll(sends).ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }
                        var elapsed = s.Elapsed;
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                        Console.Out.WriteLineAsync(
                                $"New batch of {batchSize:N0} took {elapsed} at {count:N0} ({batchSize / elapsed.TotalSeconds:N}/s)")
                            .ConfigureAwait(false);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                    }
                }
            }).ConfigureAwait(false);

            Console.ReadLine();

            cancellationTokenSource.Cancel();

            client.Dispose();
        }

        private static async Task ConsumeMessage(IAmazonSQS sqsClient, string queueUrl, int pumpNumber,
            CancellationToken token)
        {
            while (!token.IsCancellationRequested)
                try
                {
                    var receiveResult = await sqsClient.ReceiveMessageAsync(new ReceiveMessageRequest
                        {
                            MaxNumberOfMessages = 10,
                            QueueUrl = queueUrl,
                            WaitTimeSeconds = 20
                        },
                        token).ConfigureAwait(false);

                    var concurrentReceives = new List<Task>(receiveResult.Messages.Count);
                    foreach (var message in receiveResult.Messages)
                        concurrentReceives.Add(Consume(sqsClient, queueUrl, pumpNumber, message, token));

                    await Task.WhenAll(concurrentReceives)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine($"{DateTime.UtcNow} ({pumpNumber}) - cancelled");
                }
                catch (OverLimitException)
                {
                    Console.WriteLine($"{DateTime.UtcNow} ({pumpNumber}) - throttled");
                }
                catch (AmazonSQSException)
                {
                    Console.WriteLine($"{DateTime.UtcNow} ({pumpNumber}) - error");
                }
        }

        private static async Task Consume(IAmazonSQS sqsClient, string queueUrl, int pumpNumber, Message message,
            CancellationToken token)
        {
            await sqsClient.DeleteMessageAsync(queueUrl, message.ReceiptHandle, CancellationToken.None)
                .ConfigureAwait(false);
#pragma warning disable 4014
            Console.Out.WriteAsync(".");
#pragma warning restore 4014
        }

        private static async Task RetryWithBackoff(Func<Task> action, CancellationToken token, string id,
            int maxAttempts)
        {
            var attempts = 0;
            while (true)
                try
                {
                    await action()
                        .ConfigureAwait(false);
                    return;
                }
                catch (Exception ex)
                {
                    if (attempts > maxAttempts) throw new InvalidOperationException("Exhausted send retries.", ex);
                    double next;
                    lock (random)
                    {
                        next = random.NextDouble();
                    }
                    next *= 0.2; // max 20% jitter
                    next += 1D;
                    next *= 100 * Math.Pow(2, attempts++);
                    var delay = TimeSpan
                        .FromMilliseconds(
                            next); // Results in 100ms, 200ms, 400ms, 800ms, etc. including max 20% random jitter.
                    await Console.Out.WriteLineAsync($"{id}: #{attempts} {ex.Message}").ConfigureAwait(false);
                    await Task.Delay(delay, token)
                        .ConfigureAwait(false);
                }
        }

        private static async Task<string> CreateQueue(AmazonSQSClient client)
        {
            var sqsRequest = new CreateQueueRequest
            {
                QueueName = EndpointName
            };
            var createQueueResponse = await client.CreateQueueAsync(sqsRequest).ConfigureAwait(false);
            var queueUrl = createQueueResponse.QueueUrl;
            var sqsAttributesRequest = new SetQueueAttributesRequest
            {
                QueueUrl = queueUrl
            };
            sqsAttributesRequest.Attributes.Add(QueueAttributeName.MessageRetentionPeriod,
                TimeSpan.FromDays(4).TotalSeconds.ToString());

            await client.SetQueueAttributesAsync(sqsAttributesRequest).ConfigureAwait(false);
            return queueUrl;
        }
    }
}