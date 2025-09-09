'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'

export function Header() {
  const pathname = usePathname()
  const isHomePage = pathname === '/'

  return (
    <header className="border-b bg-white dark:bg-gray-900 sticky top-0 z-50" data-test-id="header">
      <div className="container mx-auto px-4">
        <div className="flex items-center justify-center h-16 relative">
          {isHomePage ? (
            <h1 
              className="text-2xl font-bold text-gray-900 dark:text-white text-center"
              data-test-id="header-title"
            >
              LaykHaus Manager
            </h1>
          ) : (
            <Link 
              href="/" 
              className="text-2xl font-bold text-gray-900 dark:text-white hover:text-blue-600 dark:hover:text-blue-400 transition-colors text-center"
              data-test-id="header-home-link"
            >
              LaykHaus Manager
            </Link>
          )}
          
          {/* Navigation breadcrumb */}
          {!isHomePage && (
            <div 
              className="absolute right-0 flex items-center space-x-2 text-sm text-gray-600 dark:text-gray-400"
              data-test-id="breadcrumb"
            >
              <Link 
                href="/" 
                className="hover:text-gray-900 dark:hover:text-gray-200"
                data-test-id="breadcrumb-home"
              >
                Home
              </Link>
              <span>/</span>
              <span className="text-gray-900 dark:text-white" data-test-id="breadcrumb-current">
                {pathname === '/connectors' && 'Connectors'}
                {pathname === '/query' && 'Query Builder'}
              </span>
            </div>
          )}
        </div>
      </div>
    </header>
  )
}