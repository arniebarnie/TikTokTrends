'use client'

import Header from '@/components/Header'
import Navigation from '@/components/Navigation'
import Section from '@/components/Section'
import Footer from '@/components/Footer'
import ProfileAnalytics from '@/components/ProfileAnalytics'
import CategoryAnalytics from '@/components/CategoryAnalytics'

export default function Home() {
  return (
    <main className="min-h-screen bg-gray-50">
      <Header />
      <Navigation />
      
      <div className="container mx-auto px-4 py-8">
        <Section 
          id="profile"
          title="By Profile"
          description="Visualize metrics grouped by TikTok profile."
        >
          <ProfileAnalytics />
        </Section>

        <Section 
          id="category"
          title="By Category"
          description="Explore trends based on TikTok video categories."
        >
          <CategoryAnalytics />
        </Section>

        <Section 
          id="language"
          title="By Language"
          description="Analyze metrics grouped by language."
        >
          <div className="bg-white p-6 rounded-lg shadow-lg min-h-[400px]" />
        </Section>
      </div>

      <Footer />
    </main>
  )
}
