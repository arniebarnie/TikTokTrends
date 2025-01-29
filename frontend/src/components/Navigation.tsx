export default function Navigation() {
  const scrollToSection = (id: string) => {
    const element = document.getElementById(id)
    element?.scrollIntoView({ behavior: 'smooth' })
  }

  return (
    <nav className="sticky top-0 bg-white shadow-sm z-50">
      <div className="container mx-auto px-4">
        <div className="flex justify-center space-x-4 py-4">
          <button
            onClick={() => scrollToSection('profile')}
            className="text-gray-600 hover:text-gray-900"
          >
            Profiles
          </button>
          <button
            onClick={() => scrollToSection('category')}
            className="text-gray-600 hover:text-gray-900"
          >
            Categories
          </button>
          <button
            onClick={() => scrollToSection('language')}
            className="text-gray-600 hover:text-gray-900"
          >
            Languages
          </button>
        </div>
      </div>
    </nav>
  )
} 