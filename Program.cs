using Dapper;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;

namespace _6_EFvsDapper_TrinhCV
{
    internal class Program
    {
        static void Main(string[] args)
        {
            try
            {
                //Connect to local db with window authenticate.
                string connectionString = ConnectionString(@"ServerName", "BookManagement");

                //Run method below to insert data to Bookmanagement DB.
                //InsertData(connectionString);

                //Use Dapper to query.
                using (var connection = new SqlConnection(connectionString))
                {
                    var sql = "SELECT c.CategoryName, b.Title, b.ISBN, b.PublicationYear, a.AuthorName" +
                        " FROM Book b" + " JOIN Category c ON b.CategoryId = c.CategoryId" +
                        " JOIN Author a ON b.AuthorId = a.AuthorId;";

                    Stopwatch stopwatch = new Stopwatch();
                    Console.WriteLine($"Starting query data...");
                    stopwatch.Start();
                    // Use the Query method to execute the query and return a list of objects    
                    var dataTable = connection.Query<ResultQuery>(sql);
                    stopwatch.Stop();
                    Console.WriteLine($"Number record is selected: {dataTable?.Count()}");
                    Console.WriteLine($"Query data successful in: {stopwatch.ElapsedMilliseconds} ms");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e.Message);
            }
        }
        public class ResultQuery
        {
            public string? CategoryName { get; set; }
            public string? Title { get; set; }
            public string? ISBN { get; set; }
            public int PublicationYear { get; set; }
            public string? AuthorName { get; set; }
        }
        public static string ConnectionString(string datasource, string database)
        {
            return @"Server=" + datasource + ";Database=" + database + ";Integrated Security=True; TrustServerCertificate=True";
        }
        public static void InsertData(string connString)
        {
            Console.WriteLine("Getting Connection ...");
            try
            {
                using (SqlConnection sourceConnection = new SqlConnection(connString))
                {
                    Console.WriteLine("Openning Connection ...");

                    sourceConnection.Open();
                    Stopwatch stopwatch = new Stopwatch();

                    DataTable categoryTable = new DataTable("Category");
                    categoryTable.Columns.Add(new DataColumn("CategoryId", typeof(int)));
                    categoryTable.Columns.Add(new DataColumn("CategoryName", typeof(string)));

                    DataTable authorTable = new DataTable("Author");
                    authorTable.Columns.Add(new DataColumn("AuthorId", typeof(int)));
                    authorTable.Columns.Add(new DataColumn("AuthorName", typeof(string)));

                    DataTable bookTable = new DataTable("Book");
                    bookTable.Columns.Add(new DataColumn("BookId", typeof(int)));
                    bookTable.Columns.Add(new DataColumn("Title", typeof(string)));
                    bookTable.Columns.Add(new DataColumn("ISBN", typeof(string)));
                    bookTable.Columns.Add(new DataColumn("PublicationYear", typeof(int)));
                    bookTable.Columns.Add(new DataColumn("CategoryId", typeof(int)));
                    bookTable.Columns.Add(new DataColumn("AuthorId", typeof(int)));

                    for (int i = 1; i <= 1000000; i++)
                    {
                        if (i <= 10)
                        {
                            categoryTable.Rows.Add(null, $"Category {i}");
                            authorTable.Rows.Add(null, $"Name {i}");
                        }
                        _ = i <= 500000 ? bookTable.Rows.Add(null, $"Title {i}", $"ISBN {i}", "2022", "1", "1") : bookTable.Rows.Add(null, $"Title {i}", $"ISBN {i}", "2023", "2", "2");
                    }

                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sourceConnection))
                    {
                        stopwatch.Start();
                        bulkCopy.BulkCopyTimeout = 600; // in seconds
                        bulkCopy.DestinationTableName = "BookManagement.dbo.Category";
                        bulkCopy.WriteToServer(categoryTable);
                        bulkCopy.DestinationTableName = "BookManagement.dbo.Author";
                        bulkCopy.WriteToServer(authorTable);
                        bulkCopy.DestinationTableName = "BookManagement.dbo.Book";
                        bulkCopy.WriteToServer(bookTable);
                        stopwatch.Stop();
                    }

                    Console.WriteLine($"Insert data successful in: {stopwatch.ElapsedMilliseconds} ms");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e.Message);
            }
        }
        //CREATE DATABASE BookManagement;
        //USE BookManagement;

        //CREATE TABLE Category
        //(
        //  CategoryId INT IDENTITY PRIMARY KEY,
        //  CategoryName NVARCHAR(100)
        //);

        //CREATE TABLE Author
        //(
        //    AuthorId INT IDENTITY PRIMARY KEY,
        //    AuthorName NVARCHAR(100)
        //);


        //CREATE TABLE Book
        //(
        //    BookId INT IDENTITY PRIMARY KEY,
        //    Title NVARCHAR(100),
        //    ISBN VARCHAR(20),
        //    PublicationYear INT,
        //    CategoryId INT REFERENCES Category(CategoryId),
        //    AuthorId INT REFERENCES Author(AuthorId)
        //);
    }
}