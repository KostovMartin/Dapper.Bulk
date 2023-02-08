using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using FluentAssertions;
using Xunit;

namespace Dapper.Bulk.Tests;

public class GuidTests : SqlServerTestSuite
{
    [Fact]
    public void InsertBulk()
    {
        var data = new List<PETranslationPhrase>();
        for (var i = 0; i < 10; i++)
        {
            data.Add(new PETranslationPhrase {
                TranslationId = i,
                CultureName = i.ToString(),
                Phrase = i.ToString(),
                PhraseHash = i % 2 == 0 ? Guid.NewGuid() : (Guid?)null,
                RowAddedDateTime = DateTime.UtcNow
            });
        }

        using var connection = GetConnection();
        connection.Open();
        var inserted = connection.BulkInsertAndSelect(data).ToList();
        for (var i = 0; i < data.Count; i++)
        {
            IsValidInsert(inserted[i], data[i]);
        }
    }

    private static void IsValidInsert(PETranslationPhrase inserted, PETranslationPhrase toBeInserted)
    {
        inserted.TranslationId.Should().BePositive();
        inserted.CultureName.Should().Be(toBeInserted.CultureName);
        inserted.Phrase.Should().Be(toBeInserted.Phrase);
        inserted.PhraseHash.Should().Be(toBeInserted.PhraseHash);
        inserted.RowAddedDateTime.Should().Be(toBeInserted.RowAddedDateTime);
    }
    
    [Table("PE_TranslationPhrase")]
    public class PETranslationPhrase
    {
        [Key]
        public virtual int TranslationId { get; set; }

        public virtual string CultureName { get; set; }

        public virtual string Phrase { get; set; }

        public virtual Guid? PhraseHash { get; set; }

        public virtual DateTime RowAddedDateTime { get; set; }
    }
}
