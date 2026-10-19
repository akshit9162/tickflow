using System.Text;

namespace Tickflow.Engine;

/// <summary>When a write counts as safe.</summary>
public enum Durability
{
    /// <summary>Written to the OS on every record; survives a process crash, not a power cut.</summary>
    None,
    /// <summary>Group commit: one fsync covers every record in a batch; acks wait for that fsync.</summary>
    Group,
    /// <summary>fsync after every single record.</summary>
    Full,
}

public sealed record RecoveryInfo(int CommandsReplayed, long ValidBytes, bool TornTailTruncated);

/// <summary>
/// Append-only command log. Record = [length u32][crc32 u32][payload]. On startup the log is read until the
/// first record that is incomplete or fails its checksum (a write cut off by a crash), and the file is truncated
/// there, so a half-written record can never be replayed.
/// </summary>
public sealed class WriteAheadLog : IDisposable
{
    private const int HeaderSize = 8;
    private const int MaxPayload = 1 << 20;

    private readonly FileStream _file;
    private readonly MemoryStream _buffer = new(256);
    private readonly BinaryWriter _writer;

    private WriteAheadLog(FileStream file, Durability durability, long records)
    {
        _file = file;
        Durability = durability;
        Records = records;
        _writer = new BinaryWriter(_buffer, Encoding.UTF8, leaveOpen: true);
    }

    public Durability Durability { get; }
    public long Records { get; private set; }

    /// <summary>Reads every valid command, truncates any torn tail, and opens the log for appending.</summary>
    public static WriteAheadLog Open(string path, Durability durability, out List<Command> commands, out RecoveryInfo info)
    {
        commands = new List<Command>();
        long valid = 0;
        bool torn = false;

        if (File.Exists(path))
        {
            var bytes = File.ReadAllBytes(path);
            var pos = 0;
            while (pos < bytes.Length)
            {
                if (bytes.Length - pos < HeaderSize) { torn = true; break; }
                var length = BitConverter.ToInt32(bytes, pos);
                var crc = BitConverter.ToUInt32(bytes, pos + 4);
                if (length <= 0 || length > MaxPayload || bytes.Length - pos - HeaderSize < length) { torn = true; break; }
                var payload = new ReadOnlySpan<byte>(bytes, pos + HeaderSize, length);
                if (Crc32.Compute(payload) != crc) { torn = true; break; }
                commands.Add(Decode(payload));
                pos += HeaderSize + length;
                valid = pos;
            }
        }

        var file = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, 64 * 1024);
        if (torn) file.SetLength(valid);
        file.Seek(valid, SeekOrigin.Begin);
        info = new RecoveryInfo(commands.Count, valid, torn);
        return new WriteAheadLog(file, durability, commands.Count);
    }

    /// <summary>Appends a command. With Full durability this fsyncs; with Group the caller calls Commit().</summary>
    public void Append(Command command)
    {
        _buffer.SetLength(0);
        _buffer.Position = HeaderSize;            // reserve the header, fill it in after encoding
        Encode(_writer, command);
        _writer.Flush();

        var span = _buffer.GetBuffer().AsSpan(0, (int)_buffer.Length);
        var payload = span[HeaderSize..];
        BitConverter.TryWriteBytes(span[..4], payload.Length);
        BitConverter.TryWriteBytes(span[4..8], Crc32.Compute(payload));

        _file.Write(span);
        Records++;
        if (Durability == Durability.Full) _file.Flush(flushToDisk: true);
        else if (Durability == Durability.None) _file.Flush(flushToDisk: false);
    }

    /// <summary>Group commit: one fsync for everything appended since the last commit.</summary>
    public void Commit()
    {
        if (Durability == Durability.Group) _file.Flush(flushToDisk: true);
    }

    public void Dispose()
    {
        _file.Flush(flushToDisk: true);
        _file.Dispose();
        _writer.Dispose();
    }

    // ---- encoding --------------------------------------------------------------------------------------

    private const byte NewOrderTag = 1;
    private const byte CancelTag = 2;

    private static void Encode(BinaryWriter w, Command command)
    {
        switch (command)
        {
            case NewOrder o:
                w.Write(NewOrderTag); w.Write(o.TimestampTicks);
                w.Write(o.ClientOrderId); w.Write(o.Symbol);
                w.Write((byte)o.Side); w.Write((byte)o.Type); w.Write(o.Price); w.Write(o.Quantity);
                break;
            case CancelOrder c:
                w.Write(CancelTag); w.Write(c.TimestampTicks); w.Write(c.Symbol); w.Write(c.OrderId);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(command));
        }
    }

    private static Command Decode(ReadOnlySpan<byte> payload)
    {
        using var r = new BinaryReader(new MemoryStream(payload.ToArray()), Encoding.UTF8);
        var tag = r.ReadByte();
        var ts = r.ReadInt64();
        return tag switch
        {
            NewOrderTag => new NewOrder(r.ReadString(), r.ReadString(), (Side)r.ReadByte(), (OrderType)r.ReadByte(),
                r.ReadInt64(), r.ReadInt64(), ts),
            CancelTag => new CancelOrder(r.ReadString(), r.ReadInt64(), ts),
            _ => throw new InvalidDataException($"unknown record tag {tag}"),
        };
    }
}

/// <summary>Standard CRC-32 (IEEE 802.3, polynomial 0xEDB88320), table-driven.</summary>
public static class Crc32
{
    private static readonly uint[] Table = BuildTable();

    public static uint Compute(ReadOnlySpan<byte> data)
    {
        var crc = 0xFFFFFFFFu;
        foreach (var b in data) crc = Table[(crc ^ b) & 0xFF] ^ (crc >> 8);
        return ~crc;
    }

    private static uint[] BuildTable()
    {
        var table = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            var c = i;
            for (var k = 0; k < 8; k++) c = (c & 1) != 0 ? 0xEDB88320u ^ (c >> 1) : c >> 1;
            table[i] = c;
        }
        return table;
    }
}
