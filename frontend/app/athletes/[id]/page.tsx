"use client";
import Image from "next/image";
import { useParams, useRouter } from "next/navigation";
import { useEffect, useState } from "react";

interface Athlete {
  id: number;
  name: string;
  gender: string;
  born: string;
  died: string | null;
  height: string;
  weight: string;
  noc: string;
  roles: string;
  game: string;
  team: string;
  sport: string;
  event: string;
  position: string;
  image_url: string | null;
}

export default function AthleteDetailsPage() {
  const params = useParams();
  const router = useRouter();
  const id = params.id;

  const [athlete, setAthlete] = useState<Athlete | null>(null);
  const [athleteEvents, setAthleteEvents] = useState<Athlete[]>([]);

  useEffect(() => {
    async function fetchAthleteDetails() {
      if (!id) return;

      try {
        const res = await fetch(
          `${process.env.NEXT_PUBLIC_API_URL}/athletes/${id}`
        );
        if (!res.ok) {
          throw new Error("Failed to fetch athlete details");
        }
        const data = await res.json();
        setAthlete(data.athlete);
        setAthleteEvents(data.events);
      } catch (error) {
        console.error(error);
      }
    }

    fetchAthleteDetails();
  }, [id]);

  const handleNavigateHome = () => {
    router.push("/"); // Navigate to home page
  };

  if (!athlete) {
    return <div>Loading...</div>;
  }

  return (
    <div className="container mx-auto p-8">
      <div className="mb-8 flex items-center justify-between">
        <div className="flex items-center">
          {athlete.image_url ? (
            <Image
              src={athlete.image_url}
              alt={athlete.name}
              width={100}
              height={100}
              className="rounded-full shadow-md"
            />
          ) : (
            <div className="flex h-24 w-24 items-center justify-center rounded-full bg-gray-300 text-xs text-gray-700">
              No Image
            </div>
          )}
          <div className="ml-6">
            <h1 className="text-3xl font-bold">{athlete.name}</h1>
            <p className="text-lg">{athlete.roles}</p>
          </div>
        </div>
        <button
          onClick={handleNavigateHome}
          className="rounded bg-olympicBlue px-4 py-2 text-white hover:bg-gray-100 focus:outline-none focus:ring-2 focus:ring-olympicBlue">
          Home
        </button>
      </div>

      <div className="mb-8">
        <h2 className="mb-4 text-2xl font-semibold">Personal Information</h2>
        <p>
          <strong>Gender:</strong> {athlete.gender}
        </p>
        <p>
          <strong>Born:</strong> {athlete.born}
        </p>
        {athlete.died && (
          <p>
            <strong>Died:</strong> {athlete.died}
          </p>
        )}
        <p>
          <strong>Height:</strong> {athlete.height}
        </p>
        <p>
          <strong>Weight:</strong> {athlete.weight}
        </p>
        <p>
          <strong>NOC:</strong> {athlete.noc}
        </p>
      </div>

      <div>
        <h2 className="mb-4 text-2xl font-semibold">Events</h2>
        <table className="min-w-full rounded-lg bg-white shadow-md">
          <thead className="bg-gray-200 text-gray-700">
            <tr>
              <th className="px-3 py-2 text-left">Game</th>
              <th className="px-3 py-2 text-left">Sport</th>
              <th className="px-3 py-2 text-left">Event</th>
              <th className="px-3 py-2 text-left">Position</th>
            </tr>
          </thead>
          <tbody>
            {athleteEvents.map((event) => (
              <tr key={`${event.event}-${event.game}`} className="border-b">
                <td className="px-3 py-2">{event.game}</td>
                <td className="px-3 py-2">{event.sport}</td>
                <td className="px-3 py-2">{event.event}</td>
                <td className="px-3 py-2">{event.position}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
