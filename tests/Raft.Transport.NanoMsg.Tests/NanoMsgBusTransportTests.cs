// Copyright (c) marcschier. Licensed under the MIT License.

namespace Raft.Transport.NanoMsg.Tests;

public sealed class NanoMsgBusTransportTests
{
    [Test]
    public async Task Bus_Transports_Exchange_Frames_Over_Inproc()
    {
        string address = "inproc://raft-bus-" + Guid.NewGuid().ToString("N");
        await using var receiver = new NanoMsgBusTransport(new NanoMsgBusTransportOptions { BindAddress = address });
        var senderOptions = new NanoMsgBusTransportOptions();
        senderOptions.Peers.Add(address);
        await using var sender = new NanoMsgBusTransport(senderOptions);
        var received = new TaskCompletionSource<byte[]>();
        receiver.FrameReceived += frame => received.TrySetResult(frame.ToArray());

        await receiver.StartAsync();
        await sender.StartAsync();

        byte[] payload = [1, 2, 3, 4];
        byte[] actual = await SendUntilReceivedAsync(sender, payload, received.Task);

        await Assert.That(actual).IsEquivalentTo(payload);
    }

    [Test]
    public async Task SendAsync_Throws_When_Not_Started()
    {
        await using var transport = new NanoMsgBusTransport(
            new NanoMsgBusTransportOptions { BindAddress = "inproc://raft-bus-not-started" });

        await Assert.That(async () => await transport.SendAsync(1, new byte[] { 1, 2, 3 }))
            .Throws<InvalidOperationException>();
    }

    private static async Task<byte[]> SendUntilReceivedAsync(
        NanoMsgBusTransport sender,
        byte[] payload,
        Task<byte[]> received)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!timeout.IsCancellationRequested)
        {
            await sender.SendAsync(1, payload, timeout.Token);
            Task completed = await Task.WhenAny(received, Task.Delay(50, timeout.Token));
            if (ReferenceEquals(completed, received))
            {
                return await received;
            }
        }

        return await received.WaitAsync(TimeSpan.FromMilliseconds(1));
    }
}
