using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace fileTest
{
    internal class Program
    {
        #region Members

        private const int numberOfFiles = 48; // number of logical cpu's core in system - 48 (threadripper 3960) on my comp.
        private const int numOfWords = 1000;
        private const int wordSize = 8;
        private const int totalSize = wordSize * numOfWords * numberOfFiles;
        private const int bufferSize = 1000;

        #endregion

        #region Main
        public static void Main(string[] args)
        {
            var path = Path.Combine(AppContext.BaseDirectory, "test.txt");
            if (File.Exists(path))
                File.Delete(path);

            // generate files
            Parallel.For(0, numberOfFiles, GenerateFile);

            //test writeToFile function
            using (var ms = new MemoryStream())
            {
                WriteToStream(ms, 0, (sp, stream) => stream.Write(sp));
                Debug.Assert(ms.Length == wordSize * numOfWords);
            }

            WriteToStorage(path);
            // WriteToStorageNoLockFail(path);
            // AppendToStorage(path);
            // WriteToStorageSequentially(path);
            //WriteToStorageSequentially2(path);

            Console.ReadKey();
        }
        #endregion

        #region Api

        // writes files to storage. Different parts of file could be written not sequentially.
        public static void WriteToStorage(string filePath)
        {
            if (File.Exists(filePath))
                File.Delete(filePath);

            var byteBuffer = new byte[totalSize];
            Array.Fill<byte>(byteBuffer, 0);
            var lockObj = new object();
            using var fs = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            fs.Write(byteBuffer, 0, totalSize);
            fs.Flush(true);
            fs.Position = 0;
            Parallel.For(0, numberOfFiles, i => WriteToStream(fs, i, (sp, stream) =>
                    {
                        lock (lockObj)
                        {
                            Console.WriteLine($"{i}  - {stream.Position}");
                            stream.Write(sp);
                        }
                    }
                )
            );
            fs.Flush(true);
        }

        // this fails from time to time, when succeeds writes file's chunks randomly.
        public static void WriteToStorageNoLockFail(string filePath)
        {
            using var fs = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            fs.SetLength(totalSize);
            fs.Position = 0;
            Parallel.For(0, numberOfFiles, i => WriteToStream(fs, i, (sp, stream) =>
                    {
                        Console.WriteLine($"{i}  - {stream.Position}");
                        stream.Write(sp);
                    }
                )
            );
            fs.Flush(true);
        }

        // allocates a place in a storage in advance according to index, so all files will be written in the same order as index.
        public static void WriteToStorageSequentially(string filePath)
        {
            var byteBuffer = new byte[totalSize];
            Array.Fill<byte>(byteBuffer, 0);
            var lockObj = new object();
            using var fs = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            fs.Write(byteBuffer, 0, totalSize);
            fs.Flush(true);
            fs.Position = 0;
            Parallel.For(0, numberOfFiles, i =>
                {
                    // acquire position in storage
                    long pos = i * wordSize * numOfWords;
                    WriteToStream(fs, i, (sp, stream) =>
                        {
                            lock (lockObj)
                            {
                                // reset position
                                fs.Position = pos;
                                Console.WriteLine($"{i}  - {stream.Position}");
                                stream.Write(sp);
                                // advance position
                                pos += sp.Length;
                            }
                        }
                    );
                }
            );
            fs.Flush(true);
        }
        // allocates a place in a storage in advance according to captured position in a storage. All files are written sequentially but in random order.
        public static void WriteToStorageSequentially2(string filePath)
        {
            // create a file of given size , file it with nulls
            var byteBuffer = new byte[totalSize];
            Array.Fill<byte>(byteBuffer, 0);
            var lockObj = new object();
            using var fs = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            fs.SetLength(totalSize);
            fs.Position = 0;
            long position = 0;

            Parallel.For(0, numberOfFiles, i =>
                {
                    // acquire position in storage
                    long pos;
                    lock (lockObj)
                    {
                        pos = position;
                        position += wordSize * numOfWords;
                    }

                    WriteToStream(fs, i, (sp, stream) =>
                        {
                            lock (lockObj)
                            {
                                // reset position
                                fs.Position = pos;
                                Console.WriteLine($"{i}  - {stream.Position}");
                                stream.Write(sp);
                                // advance position
                                pos += sp.Length;
                            }
                        }
                    );
                }
            );
            fs.Flush(true);
        }


        //Same as WriteToStorageSequentially2 but uses SyncStream. allocates a place in a storage in advance according to captured position in a storage. All files are written sequentially but in random order.
        public static void WriteToStorageSequentially3(string filePath)
        {
            // create a file of given size , file it with nulls
            var byteBuffer = new byte[totalSize];
            Array.Fill<byte>(byteBuffer, 0);
            var lockObj = new object();
            using var fs = Stream.Synchronized(new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite));
            fs.SetLength(totalSize);
            fs.Position = 0;
            long position = 0;

            Parallel.For(0, numberOfFiles, i =>
                {
                    // acquire position in storage
                    long pos;
                    lock (lockObj)
                    {
                        pos = position;
                        position += wordSize * numOfWords;
                    }

                    WriteToStream(fs, i, (sp, stream) =>
                        {
                            lock (lockObj)
                            {
                                // reset position
                                fs.Position = pos;
                                Console.WriteLine($"{i}  - {stream.Position}");
                                stream.Write(sp);
                                // advance position
                                pos += sp.Length;
                            }
                        }
                    );
                }
            );
        }

        // appends files to storage in random order. File's chunks are written randomly.
        public static void AppendToStorage(string filePath)
        {
            using var fs = new FileStream(filePath, FileMode.Append);
            Parallel.For(0, numberOfFiles, i => WriteToStream(fs, i, (sp, stream) =>
                    {
                        Console.WriteLine($"{i}  - {stream.Position}");
                        stream.Write(sp);
                    }
                )
            );
            fs.Flush(true);
        }

        #endregion

        #region Helpers

        // Generates file and save it on a disk.
        private static void GenerateFile(int i)
        {
            var name = $"File{i}";
            var fileName = $"{name}.txt";
            // word is 8 chars just to keep calculation easier
            var word = (name + (i > 9 ? " \n" : "  \n")).ToCharArray();

            var str = string.Create(word.Length * numOfWords, word, (c, seed) =>
                {
                    for (var n = 0; n < numOfWords; n++)
                    for (var m = 0; m < seed.Length; m++)
                        c[m + n * seed.Length] = seed[m];
                }
            );
            var path = Path.Combine(AppContext.BaseDirectory, fileName);
            if (File.Exists(path))
                File.Delete(path);
            File.WriteAllText(path, str);
            Console.WriteLine($"{fileName} generated.");
        }

        // writes file with index i to stream.
        private static void WriteToStream(Stream stream, int i, ReadOnlySpanAction<byte, Stream> f)
        {
            var fileName = $"File{i}.txt";
            var path = Path.Combine(AppContext.BaseDirectory, fileName);

            using var fileStream = File.OpenRead(path);
            var readFromFile = fileStream.Length;
            //read from file
            Span<byte> buff = new byte[bufferSize];
            while (readFromFile > 0)
            {
                var left = buff.Length;
                var read = 0;
                int readOnce;
                do
                {
                    readOnce = fileStream.Read(buff);
                    left -= readOnce;
                    read += readOnce;
                } while (left > 0 && readOnce > 0);

                ReadOnlySpan<byte> sp = buff.Slice(0, read);
                f(sp, stream);
                readFromFile -= read;
            }
        }

        #endregion
    }
}