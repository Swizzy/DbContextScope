/*
 * Copyright (C) 2014 Mehdi El Gueddari
 * http://mehdi.me
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE file for details.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Mehdime.Entity.Core.Interfaces;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace Mehdime.Entity.Core.Implementations
{
    /// <summary>
    /// As its name suggests, DbContextCollection maintains a collection of DbContext instances.
    ///
    /// What it does in a nutshell:
    /// - Lazily instantiates DbContext instances when its Get Of TDbContext () method is called
    /// (and optionally starts an explicit database transaction).
    /// - Keeps track of the DbContext instances it created so that it can return the existing
    /// instance when asked for a DbContext of a specific type.
    /// - Takes care of committing / rolling back changes and transactions on all the DbContext
    /// instances it created when its Commit() or Rollback() method is called.
    ///
    /// </summary>
    public class DbContextCollection : IDbContextCollection
    {
        private bool _disposed;
        private bool _completed;
        private readonly bool _readOnly;
        private readonly IsolationLevel? _isolationLevel;
        private readonly IDbContextFactory _dbContextFactory;
        private readonly Dictionary<DbContext, IDbContextTransaction> _transactions = new Dictionary<DbContext, IDbContextTransaction>();

        internal Dictionary<Type, DbContext> InitializedDbContexts { get; } = new Dictionary<Type, DbContext>();

        public DbContextCollection(bool readOnly = false, IsolationLevel? isolationLevel = null, IDbContextFactory dbContextFactory = null)
        {
            _readOnly = readOnly;
            _isolationLevel = isolationLevel;
            _dbContextFactory = dbContextFactory;
        }

        public TDbContext Get<TDbContext>() where TDbContext : DbContext, new()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException("DbContextCollection");
            }

            Type requestedType = typeof(TDbContext);

            if (!InitializedDbContexts.ContainsKey(requestedType))
            {
                // First time we've been asked for this particular DbContext type.
                // Create one, cache it and start its database transaction if needed.
                TDbContext dbContext = _dbContextFactory != null ? _dbContextFactory.CreateDbContext<TDbContext>() : Activator.CreateInstance<TDbContext>();

                InitializedDbContexts.Add(requestedType, dbContext);

                if (_readOnly)
                {
                    dbContext.ChangeTracker.AutoDetectChangesEnabled = false;
                }

                if (_isolationLevel.HasValue)
                {
                    IDbContextTransaction tran = dbContext.Database.BeginTransaction(_isolationLevel.Value);
                    _transactions.Add(dbContext, tran);
                }
            }

            return InitializedDbContexts[requestedType]  as TDbContext;
        }

        public int Commit()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException("DbContextCollection");
            }
            if (_completed)
            {
                throw new InvalidOperationException("You can't call Commit() or Rollback() more than once on a DbContextCollection." +
                                                    " All the changes in the DbContext instances managed by this collection have already been saved or" +
                                                    " rollback and all database transactions have been completed and closed. If you wish to make more data" +
                                                    " changes, create a new DbContextCollection and make your changes there.");
            }

            // Best effort. You'll note that we're not actually implementing an atomic commit
            // here. It entirely possible that one DbContext instance will be committed successfully
            // and another will fail. Implementing an atomic commit would require us to wrap
            // all of this in a TransactionScope. The problem with TransactionScope is that
            // the database transaction it creates may be automatically promoted to a
            // distributed transaction if our DbContext instances happen to be using different
            // databases. And that would require the DTC service (Distributed Transaction Coordinator)
            // to be enabled on all of our live and dev servers as well as on all of our dev workstations.
            // Otherwise the whole thing would blow up at runtime.

            // In practice, if our services are implemented following a reasonably DDD approach,
            // a business transaction (i.e. a service method) should only modify entities in a single
            // DbContext. So we should never find ourselves in a situation where two DbContext instances
            // contain uncommitted changes here. We should therefore never be in a situation where the below
            // would result in a partial commit.

            ExceptionDispatchInfo lastError = null;

            int toReturn = 0;

            foreach (DbContext dbContext in InitializedDbContexts.Values)
            {
                try
                {
                    if (_readOnly == false)
                    {
                        toReturn += dbContext.SaveChanges();
                    }

                    // If we've started an explicit database transaction, time to commit it now.
                    IDbContextTransaction tran = GetValueOrDefault(_transactions, dbContext);
                    if (tran != null)
                    {
                        tran.Commit();
                        tran.Dispose();
                    }
                }
                catch (Exception e)
                {
                    lastError = ExceptionDispatchInfo.Capture(e);
                }
            }

            _transactions.Clear();
            _completed = true;

            lastError?.Throw(); // Re-throw while maintaining the exception's original stack track

            return toReturn;
        }

        public Task<int> CommitAsync()
        {
            return CommitAsync(CancellationToken.None);
        }

        public async Task<int> CommitAsync(CancellationToken cancelToken)
        {
            if (cancelToken == null)
            {
                throw new ArgumentNullException(nameof(cancelToken));
            }
            if (_disposed)
            {
                throw new ObjectDisposedException("DbContextCollection");
            }
            if (_completed)
            {
                throw new InvalidOperationException("You can't call Commit() or Rollback() more than once on a DbContextCollection. " +
                                                    "All the changes in the DbContext instances managed by this collection have already " +
                                                    "been saved or rollback and all database transactions have been completed and closed. " +
                                                    "If you wish to make more data changes, create a new DbContextCollection and make your changes there.");
            }

            // See comments in the sync version of this method for more details.

            ExceptionDispatchInfo lastError = null;

            int toReturn = 0;

            foreach (DbContext dbContext in InitializedDbContexts.Values)
            {
                try
                {
                    if (_readOnly == false)
                    {
                        toReturn += await dbContext.SaveChangesAsync(cancelToken).ConfigureAwait(false);
                    }

                    // If we've started an explicit database transaction, time to commit it now.
                    IDbContextTransaction tran = GetValueOrDefault(_transactions, dbContext);
                    if (tran != null)
                    {
                        await tran.CommitAsync(cancelToken).ConfigureAwait(false);
                        tran.Dispose();
                    }
                }
                catch (Exception e)
                {
                    lastError = ExceptionDispatchInfo.Capture(e);
                }
            }

            _transactions.Clear();
            _completed = true;

            lastError?.Throw(); // Re-throw while maintaining the exception's original stack track

            return toReturn;
        }

        public void Rollback()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException("DbContextCollection");
            }

            if (_completed)
            {
                throw new InvalidOperationException("You can't call Commit() or Rollback() more than once on a DbContextCollection. " +
                                                    "All the changes in the DbContext instances managed by this collection have already " +
                                                    "been saved or rollback and all database transactions have been completed and closed. " +
                                                    "If you wish to make more data changes, create a new DbContextCollection and make your changes there.");
            }

            ExceptionDispatchInfo lastError = null;

            foreach (DbContext dbContext in InitializedDbContexts.Values)
            {
                // There's no need to explicitly rollback changes in a DbContext as
                // DbContext doesn't save any changes until its SaveChanges() method is called.
                // So "rolling back" for a DbContext simply means not calling its SaveChanges()
                // method.

                // But if we've started an explicit database transaction, then we must roll it back.
                IDbContextTransaction tran = GetValueOrDefault(_transactions, dbContext);
                if (tran != null)
                {
                    try
                    {
                        tran.Rollback();
                        tran.Dispose();
                    }
                    catch (Exception e)
                    {
                        lastError = ExceptionDispatchInfo.Capture(e);
                    }
                }
            }

            _transactions.Clear();
            _completed = true;

            lastError?.Throw(); // Re-throw while maintaining the exception's original stack track
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            if (_completed == false)
            {
                if (_readOnly)
                {
                    Commit();
                }
                else
                {
                    Rollback();
                }
            }

            foreach (DbContext dbContext in InitializedDbContexts.Values)
            {
                dbContext.Dispose();
            }

            InitializedDbContexts.Clear();
            _disposed = true;
        }

        /// <summary>
        /// Returns the value associated with the specified key or the default
        /// value for the TValue  type.
        /// </summary>
        private static TValue GetValueOrDefault<TKey, TValue>(IDictionary<TKey, TValue> dictionary, TKey key)
        {
            return dictionary.TryGetValue(key, out TValue value) ? value : default(TValue);
        }
    }
}