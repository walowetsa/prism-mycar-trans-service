import { Channels } from '@/lib/Channels';
import Link from 'next/link';

const ChannelSelector = () => {
  return (
    <div className='flex-1 max-h-[calc(100vh-136px)] p-4'>
        <h5 className='text-[#4ecca3] text-xl'>Channel Selection</h5>
      <ul className='mt-4 flex flex-col flex-1 rounded border-2 border-gray-300 overflow-x-hidden overflow-y-scroll max-h-[calc(100vh-136px)] [&::-webkit-scrollbar]:w-2
  [&::-webkit-scrollbar-track]:rounded-full
  [&::-webkit-scrollbar-track]:bg-gray-100
  [&::-webkit-scrollbar-thumb]:rounded-full
  [&::-webkit-scrollbar-thumb]:bg-gray-300
  dark:[&::-webkit-scrollbar-track]:bg-neutral-700
  dark:[&::-webkit-scrollbar-thumb]:bg-neutral-500'>
        {Channels.map((channel, index) => (
          <li key={channel.id} className={`flex text-lg h-12 p-2 ${index !== Channels.length && 'border-b-2 hover:bg-[#4ecca3] hover:text-black'}`}>
            {channel.active ? (
              <Link 
                href={channel.href}
                className="flex-1 cursor-pointer"
              >
                {channel.title}
              </Link>
            ) : (
              <span className="flex-1 text-gray-500 cursor-not-allowed">
                {channel.title} - INACTIVE
              </span>
            )}
          </li>
        ))}
      </ul>
    </div>
  )
}

export default ChannelSelector