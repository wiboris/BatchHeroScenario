// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Drawing;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Compute.Batch;
using Azure.Core;
using Azure.Identity;

namespace HeroScenario
{
    internal class Program
    {
        /// <summary>
        /// Simple Batch Sceanrio to Create a Batch Pool with a single Batch Node and run a task on it 
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {
            var poolID = "TestPool";
            var jobID = "TestJob";
            var taskID = "TestTask";
            string commandLine = "cmd /c echo Hello World";

            // Initialize the Batch Client
            var credential = new DefaultAzureCredential();
            Uri uri = new Uri("https://dotnotsdkbatchaccount1.eastus.batch.azure.com");
            var client = new BatchClient(uri, credential);

            // Create a Batch pool
            await CreatePool(poolID, client);

            // wait for pool to reach desired state
            await WaitForPoolToReachStateAsync(client, poolID, AllocationState.Steady);

            // Create a Batch Job
            await CreateJob(poolID, jobID, client);

            // Create a Batch Task
            await CreateTask(jobID, taskID, commandLine, client);

            // wait for task to complete
            await WaitForTaskToCompleteAsync(client, jobID, taskID);

            // Verify task
            BatchTask task = await client.GetTaskAsync(jobID, taskID);
            Assert.AreEqual(task.ExecutionInfo.Result.Value, BatchTaskExecutionResult.Success);

            // Cleanup
            await client.DeleteJobAsync(jobID);
            await client.DeletePoolAsync(poolID);
        }
      
        private static async Task CreatePool(string poolID, BatchClient client)
        {
            ImageReference imageReference = new ImageReference()
            {
                Publisher = "MicrosoftWindowsServer",
                Offer = "WindowsServer",
                Sku = "2019-datacenter-smalldisk",
                Version = "latest"
            };
            VirtualMachineConfiguration virtualMachineConfiguration = new VirtualMachineConfiguration(imageReference, "batch.node.windows amd64");

            BatchPoolCreateContent batchPoolCreateOptions = new BatchPoolCreateContent(poolID, "STANDARD_D1_v2")
            {
                VirtualMachineConfiguration = virtualMachineConfiguration,
                TargetDedicatedNodes = 1,
            };
            await client.CreatePoolAsync(batchPoolCreateOptions);
        }
      
        private static async Task CreateJob(string poolID, string jobID, BatchClient client)
        {
            BatchPoolInfo batchPoolInfo = new BatchPoolInfo()
            {
                PoolId = poolID
            };
            BatchJobCreateContent batchJobCreateContent = new BatchJobCreateContent(jobID, batchPoolInfo);
            await client.CreateJobAsync(batchJobCreateContent);
        }

        private static async Task CreateTask(string jobID, string taskID, string commandLine, BatchClient client)
        {
            RequestContent content = RequestContent.Create(new
            {
                id = taskID,
                commandLine = commandLine,
            });
            await client.CreateTaskAsync(jobID, content);
        }

        public static async Task WaitForPoolToReachStateAsync(BatchClient client, string poolId, AllocationState targetAllocationState)
        {
            DateTime allocationWaitStartTime = DateTime.UtcNow;
            DateTime timeoutAfterThisTimeUtc = allocationWaitStartTime.Add(TimeSpan.FromMinutes(10));

            BatchPool pool = await client.GetPoolAsync(poolId);

            while (pool.AllocationState != targetAllocationState)
            {
                await Task.Delay(TimeSpan.FromSeconds(10)).ConfigureAwait(continueOnCapturedContext: false);

                pool = await client.GetPoolAsync(poolId);

                if (DateTime.UtcNow > timeoutAfterThisTimeUtc)
                {
                    throw new Exception("RefreshBasedPollingWithTimeout: Timed out waiting for condition to be met.");
                }
            }
        }

        public static async Task WaitForTaskToCompleteAsync(BatchClient client, string jobId, string taskID)
        {
            DateTime allocationWaitStartTime = DateTime.UtcNow;
            DateTime timeoutAfterThisTimeUtc = allocationWaitStartTime.Add(TimeSpan.FromMinutes(10));

            BatchTask task = await client.GetTaskAsync(jobId, taskID);

            while (task.State != BatchTaskState.Completed)
            {
                await Task.Delay(TimeSpan.FromSeconds(10)).ConfigureAwait(continueOnCapturedContext: false);

                task = await client.GetTaskAsync(jobId, taskID);

                if (DateTime.UtcNow > timeoutAfterThisTimeUtc)
                {
                    throw new Exception("RefreshBasedPollingWithTimeout: Timed out waiting for condition to be met.");
                }
            }
        }

    }
}
