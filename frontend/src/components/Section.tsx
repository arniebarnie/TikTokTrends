interface SectionProps {
  id: string
  title: string
  description: string
  children: React.ReactNode
}

export default function Section({ id, title, description, children }: SectionProps) {
  return (
    <section id={id} className="mb-12">
      <h2 className="text-2xl font-semibold text-gray-800 mb-2">{title}</h2>
      <p className="text-gray-600 mb-6">{description}</p>
      {children}
    </section>
  )
} 