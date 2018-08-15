using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;

namespace EFContainsBenchmark.Model
{
    public class BenchmarkContext : DbContext
    {

        public DbSet<Security> Securities { get; set; }

        public DbSet<PriceSource> PriceSources { get; set; }

        public DbSet<Price> Prices { get; set; }

        public DbSet<SharedQueryModel> QueryDataShared { get; set; }

        public DbSet<EntityFrameworkCore.MemoryJoin.QueryModelClass> QueryData { get; set; }

        public BenchmarkContext(DbContextOptions<BenchmarkContext> options) : base(options) { }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<PriceSource>().ToTable("PriceSource");
            modelBuilder.Entity<PriceSource>().HasKey(x => x.PriceSourceId);

            modelBuilder.Entity<Price>().ToTable("Price");
            modelBuilder.Entity<Price>().HasKey(x => x.PriceId);
            modelBuilder.Entity<Price>().HasAlternateKey(x => new { x.SecurityId, x.TradedOn, x.PriceSourceId });

            modelBuilder.Entity<Security>().ToTable("Security");
            modelBuilder.Entity<Security>().HasKey(x => x.SecurityId);
            modelBuilder.Entity<Security>().HasIndex(x => x.Ticker);

            modelBuilder.Entity<SharedQueryModel>().ToTable("SharedQueryData");
            modelBuilder.Entity<SharedQueryModel>().HasKey(x => new { x.Ticker, x.PriceSourceId, x.TradedOn });

            base.OnModelCreating(modelBuilder);
        }

    }
}
