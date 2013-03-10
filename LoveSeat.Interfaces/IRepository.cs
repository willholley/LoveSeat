using System;
using System.Threading.Tasks;
using LoveSeat.Interfaces;

namespace LoveSeat.Interfaces
{
    public interface IRepository<T> where T : IBaseObject
    {
        Task SaveAsync(T item);
        Task<T> FindAsync(Guid id);
    }
}