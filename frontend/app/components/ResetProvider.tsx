// app/components/ResetProvider.tsx

'use client';

import { useRouter } from 'next/router';
import { useEffect } from 'react';

interface ResetProviderProps {
  children: React.ReactNode;
}

export default function ResetProvider({ children }: ResetProviderProps) {
  const router = useRouter();

  useEffect(() => {
    const handleRouteChange = (url: string) => {
      if (url === '/') {
        // Reset logic when navigating back to the home page
        // Implement your reset logic here, e.g., dispatching actions to reset filters and table data
        console.log('Resetting filters and table state.');
      }
    };

    router.events.on('routeChangeComplete', handleRouteChange);

    return () => {
      router.events.off('routeChangeComplete', handleRouteChange);
    };
  }, [router]);

  return <>{children}</>;
}
