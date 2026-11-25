// Copyright (c) marcschier. Licensed under the MIT License.

using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Raft.Messages;

namespace Raft.Transport.Tests;

public sealed class TlsRaftTransportTests
{
    [Test]
    public async Task PendingTlsDataDoesNotInterruptOrderedSends()
    {
        using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        using X509Certificate2 first = CreateCertificate();
        using X509Certificate2 second = CreateCertificate();
        using var listener = new TcpListener(Loopback());
        listener.Start();
        await using var source = new TlsRaftTransport(1, Loopback(), first,
            [new(2, (IPEndPoint)listener.LocalEndpoint, second)]);
        await source.StartAsync(limit.Token);
        byte[] outbound = Heartbeat(1, 2, 1);
        await source.SendAsync(2, outbound, limit.Token);

        using TcpClient socket = await listener.AcceptTcpClientAsync(limit.Token);
        using var stream = new SslStream(socket.GetStream(), false, (_, remote, _, _) => IsCertificate(first, remote));
        await stream.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
        {
            ServerCertificate = second,
            ClientCertificateRequired = true,
            EnabledSslProtocols = SslProtocols.Tls12,
        }, limit.Token);
        await Assert.That(await ReadFrameAsync(stream, limit.Token)).IsEquivalentTo(outbound);

        // Leave an encrypted record pending on the sender, as TLS post-handshake records can be.
        await stream.WriteAsync(new byte[] { 42 }, limit.Token);
        byte[] next = Heartbeat(1, 2, 2);
        byte[] last = Heartbeat(1, 2, 3);
        await source.SendAsync(2, next, limit.Token);
        await source.SendAsync(2, last, limit.Token);

        await Assert.That(await ReadFrameAsync(stream, limit.Token)).IsEquivalentTo(next);
        await Assert.That(await ReadFrameAsync(stream, limit.Token)).IsEquivalentTo(last);
        await Assert.That(listener.Pending()).IsFalse();
    }

    [Test]
    public async Task ConfiguredMembersExchangeFramesInOrder()
    {
        using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        using X509Certificate2 first = CreateCertificate();
        using X509Certificate2 second = CreateCertificate();
        IPEndPoint firstEndPoint = AvailableEndPoint();
        var firstChecks = new ConcurrentQueue<ulong>();
        await using var target = new TlsRaftTransport(2, Loopback(), second, [new(1, firstEndPoint, first)]);
        await target.StartAsync(limit.Token);
        await using var source = new TlsRaftTransport(1, firstEndPoint, first,
            [new(2, Loopback(target.BoundPort), second)],
            new TlsRaftTransportOptions
            {
                ValidatePeerCertificate = (id, _, _, _) =>
                {
                    firstChecks.Enqueue(id);
                    return true;
                },
            });

        var firstReceived = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondReceived = new TaskCompletionSource<byte[][]>(TaskCreationOptions.RunContinuationsAsynchronously);
        var frames = new ConcurrentQueue<byte[]>();
        source.FrameReceived += frame => firstReceived.TrySetResult(frame.ToArray());
        target.FrameReceived += frame =>
        {
            frames.Enqueue(frame.ToArray());
            if (frames.Count == 2)
            {
                secondReceived.TrySetResult(frames.ToArray());
            }
        };
        await source.StartAsync(limit.Token);

        byte[] outbound = Heartbeat(1, 2, 1);
        byte[] next = Heartbeat(1, 2, 2);
        byte[] response = Heartbeat(2, 1, 1);
        await Assert.That(async () => await source.SendAsync(2, Heartbeat(2, 2), limit.Token))
            .Throws<ArgumentException>();
        await Assert.That(async () => await source.SendAsync(2, Heartbeat(1, 3), limit.Token))
            .Throws<ArgumentException>();
        await source.SendAsync(2, outbound, limit.Token);
        await source.SendAsync(2, next, limit.Token);
        await target.SendAsync(1, response, limit.Token);

        byte[][] delivered = await secondReceived.Task.WaitAsync(limit.Token);
        await Assert.That(delivered[0]).IsEquivalentTo(outbound);
        await Assert.That(delivered[1]).IsEquivalentTo(next);
        await Assert.That(await firstReceived.Task.WaitAsync(limit.Token)).IsEquivalentTo(response);
        await Assert.That(firstChecks.All(id => id == 2) && !firstChecks.IsEmpty).IsTrue();
    }

    [Test]
    [Arguments(3ul, 2ul)]
    [Arguments(1ul, 3ul)]
    public async Task InboundRoutingMustMatchTheAuthenticatedSenderAndLocalRecipient(ulong from, ulong to)
    {
        using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        using X509Certificate2 first = CreateCertificate();
        using X509Certificate2 second = CreateCertificate();
        using X509Certificate2 third = CreateCertificate();
        await using var target = new TlsRaftTransport(2, Loopback(), second,
            [new(1, Loopback(1), first), new(3, Loopback(1), third)]);
        int delivered = 0;
        target.FrameReceived += _ => Interlocked.Increment(ref delivered);
        await target.StartAsync(limit.Token);

        using var client = new TcpClient();
        using SslStream stream = await ConnectAsync(client, target.BoundPort, first, second, limit.Token);
        await WriteFrameAsync(stream, Heartbeat(from, to), limit.Token);
        await Assert.That(await ReadOrCloseAsync(stream, limit.Token)).IsEqualTo(0);
        await Assert.That(Volatile.Read(ref delivered)).IsEqualTo(0);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task InboundAuthenticationRequiresBothThePinAndTheAdditionalPolicy(bool pinned)
    {
        using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        using X509Certificate2 first = CreateCertificate();
        using X509Certificate2 second = CreateCertificate();
        using X509Certificate2 third = CreateCertificate();
        int policyCalls = 0;
        var options = new TlsRaftTransportOptions
        {
            ValidatePeerCertificate = (_, _, _, _) =>
            {
                Interlocked.Increment(ref policyCalls);
                return false;
            },
        };
        await using var target = new TlsRaftTransport(2, Loopback(), second, [new(1, Loopback(1), first)], options);
        int delivered = 0;
        target.FrameReceived += _ => Interlocked.Increment(ref delivered);
        await target.StartAsync(limit.Token);

        using var client = new TcpClient();
        bool rejected = false;
        try
        {
            using SslStream stream = await ConnectAsync(client, target.BoundPort, pinned ? first : third, second,
                limit.Token);
            await WriteFrameAsync(stream, Heartbeat(1, 2), limit.Token);
            rejected = await ReadOrCloseAsync(stream, limit.Token) == 0;
        }
        catch (AuthenticationException)
        {
            rejected = true;
        }
        catch (IOException)
        {
            rejected = true;
        }

        await Assert.That(rejected).IsTrue();
        await Assert.That(Volatile.Read(ref policyCalls)).IsEqualTo(pinned ? 1 : 0);
        await Assert.That(Volatile.Read(ref delivered)).IsEqualTo(0);
    }

    [Test]
    public async Task OutboundAuthenticationRejectsAnotherConfiguredPeersCertificate()
    {
        using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        using X509Certificate2 first = CreateCertificate();
        using X509Certificate2 second = CreateCertificate();
        using X509Certificate2 third = CreateCertificate();
        using var listener = new TcpListener(Loopback());
        listener.Start();
        var serverEndPoint = (IPEndPoint)listener.LocalEndpoint;
        await using var source = new TlsRaftTransport(1, Loopback(), first,
            [new(2, serverEndPoint, second), new(3, Loopback(1), third)]);
        await source.StartAsync(limit.Token);
        await source.SendAsync(2, Heartbeat(1, 2), limit.Token);

        using TcpClient socket = await listener.AcceptTcpClientAsync(limit.Token);
        using var stream = new SslStream(socket.GetStream(), false, (_, remote, _, _) => IsCertificate(first, remote));
        int bytes = 0;
        try
        {
            await stream.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
            {
                ServerCertificate = third,
                ClientCertificateRequired = true,
                EnabledSslProtocols = SslProtocols.Tls12,
            }, limit.Token);
            bytes = await ReadOrCloseAsync(stream, limit.Token);
        }
        catch (AuthenticationException)
        {
        }
        catch (IOException)
        {
        }

        await Assert.That(bytes).IsEqualTo(0);
    }

    [Test]
    public async Task StalledHandshakeDoesNotBlockAnotherPeerAndTheSendQueueRecovers()
    {
        using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        using X509Certificate2 first = CreateCertificate();
        using X509Certificate2 second = CreateCertificate();
        using X509Certificate2 third = CreateCertificate();
        using var stalledListener = new TcpListener(Loopback());
        stalledListener.Start();
        var options = new TlsRaftTransportOptions
        {
            IoTimeout = TimeSpan.FromSeconds(5),
            MaxPendingConnections = 2,
            SendQueueCapacity = 1,
        };
        await using var target = new TlsRaftTransport(2, Loopback(), second, [new(1, Loopback(1), first)], options);
        var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        target.FrameReceived += frame => received.TrySetResult(frame.ToArray());
        await target.StartAsync(limit.Token);

        using var stalledInbound = new TcpClient();
        await stalledInbound.ConnectAsync(Loopback(target.BoundPort), limit.Token);
        await using var source = new TlsRaftTransport(1, Loopback(), first,
            [new(2, Loopback(target.BoundPort), second), new(3, (IPEndPoint)stalledListener.LocalEndpoint, third)],
            options);
        await source.StartAsync(limit.Token);
        byte[] stalled = Heartbeat(1, 3);
        await source.SendAsync(3, stalled, limit.Token);
        using TcpClient firstConnection = await stalledListener.AcceptTcpClientAsync(limit.Token);
        await source.SendAsync(3, stalled, limit.Token);
        await Assert.That(async () => await source.SendAsync(3, stalled, limit.Token)).Throws<IOException>();

        byte[] healthy = Heartbeat(1, 2);
        await source.SendAsync(2, healthy, limit.Token);
        await Assert.That(await received.Task.WaitAsync(TimeSpan.FromSeconds(4), limit.Token)).IsEquivalentTo(healthy);
        using TcpClient secondConnection = await stalledListener.AcceptTcpClientAsync(limit.Token);
        await source.SendAsync(3, stalled, limit.Token);
    }

    [Test]
    public async Task OversizedMessagesAreRejectedWithoutBlockingOtherAuthenticatedPeers()
    {
        using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        using X509Certificate2 first = CreateCertificate();
        using X509Certificate2 second = CreateCertificate();
        using X509Certificate2 third = CreateCertificate();
        var options = new TlsRaftTransportOptions { MaxFrameLength = MessageCodec.EncodedLength(new Message()) };
        await using var target = new TlsRaftTransport(2, Loopback(), second,
            [new(1, Loopback(1), first), new(3, Loopback(1), third)], options);
        var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        target.FrameReceived += frame => received.TrySetResult(frame.ToArray());
        await target.StartAsync(limit.Token);
        await using var source = new TlsRaftTransport(1, Loopback(), first,
            [new(2, Loopback(target.BoundPort), second)], options);
        await source.StartAsync(limit.Token);

        byte[] append = Encode(new Message
        {
            Type = MessageType.Append,
            From = 1,
            To = 2,
            Term = 1,
            Entries = [new Entry(EntryType.Normal, 1, 1, new byte[] { 42 })],
        });
        await Assert.That(async () => await source.SendAsync(2, append, limit.Token)).Throws<ArgumentException>();

        using var raw = new TcpClient();
        using SslStream stream = await ConnectAsync(raw, target.BoundPort, first, second, limit.Token);
        await WriteFrameAsync(stream, append, limit.Token);
        await Assert.That(await ReadOrCloseAsync(stream, limit.Token)).IsEqualTo(0);

        using var other = new TcpClient();
        using SslStream otherStream = await ConnectAsync(other, target.BoundPort, third, second, limit.Token);
        byte[] heartbeat = Heartbeat(3, 2);
        await WriteFrameAsync(otherStream, heartbeat, limit.Token);
        await Assert.That(await received.Task.WaitAsync(limit.Token)).IsEquivalentTo(heartbeat);
    }

    [Test]
    public async Task IdleAuthenticatedConnectionDoesNotBlockAnotherMemberAndReleasesItsSlot()
    {
        using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        using X509Certificate2 first = CreateCertificate();
        using X509Certificate2 second = CreateCertificate();
        using X509Certificate2 third = CreateCertificate();
        var options = new TlsRaftTransportOptions { IoTimeout = TimeSpan.FromSeconds(4), MaxPendingConnections = 2 };
        await using var target = new TlsRaftTransport(2, Loopback(), second,
            [new(1, Loopback(1), first), new(3, Loopback(1), third)], options);
        var fromFirst = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        var fromThird = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        target.FrameReceived += frame =>
        {
            if (MessageCodec.TryParse(frame.Span, out Message? message) && message is not null)
            {
                (message.From == 1 ? fromFirst : fromThird).TrySetResult(frame.ToArray());
            }
        };
        await target.StartAsync(limit.Token);

        using var idle = new TcpClient();
        using SslStream idleStream = await ConnectAsync(idle, target.BoundPort, first, second, limit.Token);
        using var other = new TcpClient();
        using SslStream otherStream = await ConnectAsync(other, target.BoundPort, third, second, limit.Token);
        byte[] fromOther = Heartbeat(3, 2);
        await WriteFrameAsync(otherStream, fromOther, limit.Token);
        await Assert.That(await fromThird.Task.WaitAsync(TimeSpan.FromSeconds(3), limit.Token))
            .IsEquivalentTo(fromOther);

        await Assert.That(await ReadOrCloseAsync(idleStream, limit.Token)).IsEqualTo(0);
        using var retry = new TcpClient();
        using SslStream retryStream = await ConnectAsync(retry, target.BoundPort, first, second, limit.Token);
        byte[] fromRetried = Heartbeat(1, 2);
        await WriteFrameAsync(retryStream, fromRetried, limit.Token);
        await Assert.That(await fromFirst.Task.WaitAsync(limit.Token)).IsEquivalentTo(fromRetried);
    }

    private static IPEndPoint Loopback(int port = 0) => new(IPAddress.Loopback, port);

    private static IPEndPoint AvailableEndPoint()
    {
        using var listener = new TcpListener(Loopback());
        listener.Start();
        return (IPEndPoint)listener.LocalEndpoint;
    }

    private static X509Certificate2 CreateCertificate()
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest("CN=Raft Test Peer", key, HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            new OidCollection { new("1.3.6.1.5.5.7.3.1"), new("1.3.6.1.5.5.7.3.2") }, critical: false));
        using X509Certificate2 issued = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1),
            DateTimeOffset.UtcNow.AddDays(1));
        byte[] pkcs12 = issued.Export(X509ContentType.Pkcs12);
#if NET9_0_OR_GREATER
        return X509CertificateLoader.LoadPkcs12(pkcs12, null);
#else
        return new X509Certificate2(pkcs12);
#endif
    }

    private static bool IsCertificate(X509Certificate2 expected, X509Certificate? remote) =>
        remote is not null && expected.GetCertHash(HashAlgorithmName.SHA256)
            .AsSpan().SequenceEqual(remote.GetCertHash(HashAlgorithmName.SHA256));

    private static async Task<SslStream> ConnectAsync(
        TcpClient client,
        int port,
        X509Certificate2 certificate,
        X509Certificate2 expected,
        CancellationToken cancellationToken)
    {
        await client.ConnectAsync(Loopback(port), cancellationToken);
        var stream = new SslStream(client.GetStream(), false, (_, remote, _, _) => IsCertificate(expected, remote));
        try
        {
            await stream.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
            {
                TargetHost = IPAddress.Loopback.ToString(),
                ClientCertificates = [certificate],
                EnabledSslProtocols = SslProtocols.Tls12,
                AllowTlsResume = false,
            }, cancellationToken);
            return stream;
        }
        catch
        {
            stream.Dispose();
            throw;
        }
    }

    private static async Task<int> ReadOrCloseAsync(SslStream stream, CancellationToken cancellationToken)
    {
        try
        {
            return await stream.ReadAsync(new byte[1], cancellationToken);
        }
        catch (IOException)
        {
            return 0;
        }
    }

    private static async Task WriteFrameAsync(SslStream stream, byte[] payload, CancellationToken cancellationToken)
    {
        byte[] framed = new byte[FrameCodec.FrameLength(payload.Length)];
        FrameCodec.TryWriteFrame(payload, framed);
        await stream.WriteAsync(framed, cancellationToken);
    }

    private static async Task<byte[]> ReadFrameAsync(SslStream stream, CancellationToken cancellationToken)
    {
        byte[] prefix = new byte[sizeof(int)];
        await stream.ReadExactlyAsync(prefix, cancellationToken);
        byte[] payload = new byte[BinaryPrimitives.ReadInt32BigEndian(prefix)];
        await stream.ReadExactlyAsync(payload, cancellationToken);
        return payload;
    }

    private static byte[] Heartbeat(ulong from, ulong to, ulong term = 1) =>
        Encode(new Message { Type = MessageType.Heartbeat, From = from, To = to, Term = term });

    private static byte[] Encode(Message message)
    {
        var frame = new byte[MessageCodec.EncodedLength(message)];
        MessageCodec.TryWrite(message, frame);
        return frame;
    }
}
