"use client";

import React, { useState, useEffect, useMemo, useContext } from "react";
import Table from "./components/Table";
import FilterMenu from "./components/FilterMenu";
import OlympicGamesSelector from "./components/OlympicGamesSelector"; // Import the new component
import { FaBars } from "react-icons/fa";
import { useRouter } from "next/navigation";
import { ResetContext } from "./context/ResetContext";

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

interface GroupedAthlete {
  id: number;
  name: string;
  gender: string;
  born: string;
  died: string | null;
  height: string;
  weight: string;
  noc: string;
  roles: string;
  image_url: string | null;
  events: Array<{
    game: string;
    sport: string;
    event: string;
    team: string;
    position: string;
  }>;
}

interface NocCountry {
  noc: string;
  country: string;
}

interface HostCity {
  year: number;
  season: string;
  game: string;
  host_city: string;
}

// Fetch Athletes with Pagination and Filters
async function fetchAthletes(
  skip: number,
  limit: number,
  filters: { game?: string; sport?: string; role?: string; name?: string }
): Promise<Athlete[]> {
  const queryParams = new URLSearchParams({
    skip: skip.toString(),
    limit: limit.toString(),
  });

  if (filters.game) queryParams.append("game", filters.game);
  if (filters.sport) queryParams.append("sport", filters.sport);
  if (filters.role) queryParams.append("role", filters.role);
  if (filters.name) queryParams.append("name", filters.name);

  const res = await fetch(
    `${process.env.NEXT_PUBLIC_API_URL}/athletes?${queryParams.toString()}`,
    {
      headers: {
        Accept: "application/json",
      },
    }
  );

  if (!res.ok) {
    throw new Error("Failed to fetch athletes");
  }

  const data = await res.json();

  if (!data.athletes || !Array.isArray(data.athletes)) {
    throw new Error("Invalid data format received from server.");
  }

  return data.athletes;
}

// Fetch NOC Countries
async function fetchNocCountries(): Promise<NocCountry[]> {
  const res = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/noc-countries`);
  if (!res.ok) {
    throw new Error("Failed to fetch NOC countries");
  }
  return res.json();
}

// Fetch Host Cities (Olympic Games)
async function fetchHostCities(): Promise<HostCity[]> {
  const res = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/host-cities`);
  if (!res.ok) {
    throw new Error("Failed to fetch host cities");
  }
  return res.json();
}

export default function HomePage() {
  const router = useRouter();
  const { resetFiltersAndTable } = useContext(ResetContext);

  // State variables
  const [athletes, setAthletes] = useState<Athlete[]>([]);
  const [nocCountries, setNocCountries] = useState<NocCountry[]>([]);
  const [hostCities, setHostCities] = useState<HostCity[]>([]);
  const [searchQuery, setSearchQuery] = useState<string>("");
  const [selectedOlympics, setSelectedOlympics] = useState<string>("");
  const [selectedRole, setSelectedRole] = useState<string>("");
  const [visibleColumns, setVisibleColumns] = useState<string[]>([
    "image", // Ensure 'image' is the first column
    "name",
    "sport",
    "event",
    "position",
  ]);
  const [sortColumn, setSortColumn] = useState<string | null>(null);
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("asc");
  const [isFilterMenuOpen, setIsFilterMenuOpen] = useState<boolean>(false);
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [currentPage, setCurrentPage] = useState<number>(1);
  const [totalPages, setTotalPages] = useState<number>(1); // To be updated based on backend count
  const [isToggleOpen, setIsToggleOpen] = useState<boolean>(false);

  const ITEMS_PER_PAGE = 25; // Adjust as needed

  const handleResetAndNavigateHome = () => {
    // Reset all relevant states
    setSearchQuery("");
    setSelectedOlympics("");
    setSelectedRole("");
    setVisibleColumns(["image", "name", "sport", "event", "position"]);
    setSortColumn(null);
    setSortDirection("asc");
    setCurrentPage(1);

    // Ensure the data is re-fetched with default settings
    resetFiltersAndTable();
    router.push("/"); // Navigate to home without query params
  };

  // Group athletes by ID
  const groupedAthletes: GroupedAthlete[] = useMemo(() => {
    const map = new Map<number, GroupedAthlete>();

    athletes.forEach((athlete) => {
      if (!map.has(athlete.id)) {
        map.set(athlete.id, {
          id: athlete.id,
          name: athlete.name,
          gender: athlete.gender,
          born: athlete.born,
          died: athlete.died,
          height: athlete.height,
          weight: athlete.weight,
          noc: athlete.noc,
          roles: athlete.roles,
          image_url: athlete.image_url,
          events: [],
        });
      }

      const groupedAthlete = map.get(athlete.id)!;
      groupedAthlete.events.push({
        game: athlete.game,
        sport: athlete.sport,
        event: athlete.event,
        team: athlete.team,
        position: athlete.position,
      });
    });

    return Array.from(map.values());
  }, [athletes]);

  // Data loading logic
  useEffect(() => {
    async function loadData() {
      try {
        setIsLoading(true);
        setError(null);

        // Fetch NOC Countries
        if (nocCountries.length === 0) {
          const nocCountryData = await fetchNocCountries();
          setNocCountries(nocCountryData);
        }

        // Fetch Host Cities
        if (hostCities.length === 0) {
          const hostCityData = await fetchHostCities();
          setHostCities(hostCityData);
        }

        // Fetch total records based on current filters
        const countQueryParams = new URLSearchParams();
        if (selectedOlympics) countQueryParams.append("game", selectedOlympics);
        if (selectedRole) countQueryParams.append("role", selectedRole);
        if (searchQuery) countQueryParams.append("name", searchQuery);
        // Add more filters as needed

        const countRes = await fetch(
          `${process.env.NEXT_PUBLIC_API_URL}/athletes/count?${countQueryParams.toString()}`
        );
        if (!countRes.ok) {
          throw new Error("Failed to fetch athletes count");
        }
        const countData = await countRes.json();
        const totalRecords = countData.total_records;
        setTotalPages(Math.ceil(totalRecords / ITEMS_PER_PAGE));

        // Calculate skip based on currentPage and ITEMS_PER_PAGE
        const skip = (currentPage - 1) * ITEMS_PER_PAGE;

        // Prepare filters
        const filters: { game?: string; sport?: string; role?: string; name?: string } = {};
        if (selectedOlympics) filters.game = selectedOlympics;
        if (selectedRole) filters.role = selectedRole;
        if (searchQuery) filters.name = searchQuery;

        // Fetch athletes based on current filters and pagination
        const athleteData = await fetchAthletes(skip, ITEMS_PER_PAGE, filters);
        setAthletes(athleteData);
      } catch (err: unknown) {
        console.error(err);
        if (err instanceof Error) {
          setError(err.message);
        } else {
          setError("An unknown error occurred while fetching data.");
        }
      } finally {
        setIsLoading(false);
      }
    }

    loadData();
  }, [currentPage, searchQuery, selectedOlympics, selectedRole, nocCountries.length, hostCities.length]);

  // Handle search input change
  const handleSearch = (event: React.ChangeEvent<HTMLInputElement>) => {
    setSearchQuery(event.target.value);
    setCurrentPage(1); // Reset to first page on new search

    // Update URL with search query
    const params = new URLSearchParams(window.location.search);
    if (event.target.value) {
      params.set("search", event.target.value);
    } else {
      params.delete("search");
    }
    router.push(`/?${params.toString()}`);
  };

  // Handle Olympics selection change
  const handleOlympicsChange = (game: string) => {
    setSelectedOlympics(game);
    setCurrentPage(1); // Reset to first page on filter change

    // Update URL with selected game
    const params = new URLSearchParams(window.location.search);
    if (game) {
      params.set("games", game);
    } else {
      params.delete("games");
    }
    router.push(`/?${params.toString()}`);
  };

  // Toggle column visibility
  const handleColumnToggle = (column: string) => {
    if (column === "image") return; // Image column should always be visible
    setVisibleColumns((prevColumns) =>
      prevColumns.includes(column)
        ? prevColumns.filter((col) => col !== column)
        : [...prevColumns, column]
    );
  };

  // Handle sorting
  const handleSort = (column: string) => {
    if (sortColumn === column) {
      setSortDirection((prevDirection) => (prevDirection === "asc" ? "desc" : "asc"));
    } else {
      setSortColumn(column);
      setSortDirection("asc");
    }
  };

  // Parse and format sport data
  const parseSport = (sport: string): string => {
    if (!sport) return "N/A";
    const regex = /(.*)\s\((.*)\)/;
    const match = sport.match(regex);
    if (match) {
      const [, subSport] = match;
      return `${subSport}`;
    }
    return sport || "N/A";
  };

  // Parse and format event data
  const parseEvent = (event: string): string => {
    if (!event) return "N/A";
    const regex = /(.*)\s\(\((.*)\)\)/;
    const match = event.match(regex);
    if (match) {
      const [, eventName] = match;
      return eventName.trim() || "N/A";
    }
    return event.replace("((Olympic))", "").trim() || "N/A";
  };

  // Construct rolesList from athletes data with safety checks
  const rolesList = useMemo(() => {
    return Array.from(
      new Set(
        athletes
          .filter((a: Athlete) => typeof a.roles === "string") // Ensure roles is a string
          .flatMap((a: Athlete) => a.roles.split(" • "))
      )
    );
  }, [athletes]);

  // Separate host cities into Summer and Winter
  const summerGames = useMemo(() => {
    return hostCities.filter((game) => game.season.toLowerCase() === "summer");
  }, [hostCities]);

  const winterGames = useMemo(() => {
    return hostCities.filter((game) => game.season.toLowerCase() === "winter");
  }, [hostCities]);

  return (
    <div className="bg-olympicWhite min-h-screen relative">
      {/* Display Error State */}
      {error && (
        <div className="fixed top-0 left-0 w-full h-full bg-white bg-opacity-75 flex items-center justify-center z-50">
          <p className="text-xl text-red-500">Error: {error}</p>
        </div>
      )}

      {/* Display Loading Indicator (Non-blocking) */}
      {isLoading && (
        <div className="fixed top-4 right-4 z-50">
          <div className="loader"></div>
        </div>
      )}

      {/* Display Content Only When Not Loading and No Error */}
      {!isLoading && !error && (
        <>
          {/* Navigation Bar with Home Button */}
          <nav className="fixed top-0 left-0 w-full bg-olympicBlue text-white p-4 flex justify-between items-center z-40">
            <button
              onClick={handleResetAndNavigateHome}
              className="px-4 py-2 bg-white text-olympicBlue rounded hover:bg-gray-100 focus:outline-none focus:ring-2 focus:ring-olympicBlue"
            >
              Home
            </button>
          </nav>

          {/* Hamburger Menu Icon */}
          <button
            className="fixed top-4 left-4 z-50 p-2 bg-olympicBlue text-white rounded-full focus:outline-none"
            onClick={() => setIsFilterMenuOpen(!isFilterMenuOpen)}
            aria-label="Toggle Filter Menu"
          >
            <FaBars size={24} />
          </button>

          {/* Filter Menu */}
          {isFilterMenuOpen && (
            <FilterMenu
              searchQuery={searchQuery}
              handleSearch={handleSearch}
              selectedOlympics={selectedOlympics}
              handleOlympicsChange={handleOlympicsChange}
              summerGames={summerGames.map((game) => ({
                year: game.year.toString(),
                season: game.season,
                file: `${game.year}.svg`, // Assuming image files are named by year
              }))}
              winterGames={winterGames.map((game) => ({
                year: game.year.toString(),
                season: game.season,
                file: `${game.year}.svg`, // Assuming image files are named by year
              }))}
              selectedRole={selectedRole}
              setSelectedRole={setSelectedRole}
              rolesList={rolesList}
              visibleColumns={visibleColumns}
              handleColumnToggle={handleColumnToggle}
              isFilterMenuOpen={isFilterMenuOpen}
              setIsFilterMenuOpen={setIsFilterMenuOpen}
            />
          )}

          {/* Olympic Games Selector Component */}
          <OlympicGamesSelector
            summerGames={summerGames}
            winterGames={winterGames}
            selectedOlympics={selectedOlympics}
            setSelectedOlympics={handleOlympicsChange} // Use the handler to update URL
            isToggleOpen={isToggleOpen}
            setIsToggleOpen={setIsToggleOpen}
          />

          {/* Athletes Table */}
          <div className="flex-1 p-24 w-full"> {/* Added p-24 to account for fixed nav */}
            <Table
              data={groupedAthletes.map((athlete) => ({
                ...athlete,
                // Apply parseSport and parseEvent, with 'N/A' fallback
                sport: parseSport(athlete.events[0]?.sport || ""),
                event: parseEvent(athlete.events[0]?.event || ""),
                position: athlete.events[0]?.position || "N/A",
              }))}
              visibleColumns={visibleColumns}
              nocCountries={nocCountries}
              onSort={handleSort}
              sortColumn={sortColumn}
              sortDirection={sortDirection}
            />
          </div>

          {/* Pagination Controls */}
          <div className="flex justify-center items-center my-4">
            <button
              className="mx-2 px-4 py-2 bg-olympicBlue text-white rounded disabled:opacity-50"
              onClick={() => setCurrentPage((prev) => Math.max(prev - 1, 1))}
              disabled={currentPage === 1}
            >
              Previous
            </button>
            <span className="mx-2">
              Page {currentPage} of {totalPages}
            </span>
            <button
              className="mx-2 px-4 py-2 bg-olympicBlue text-white rounded disabled:opacity-50"
              onClick={() => setCurrentPage((prev) => Math.min(prev + 1, totalPages))}
              disabled={currentPage === totalPages}
            >
              Next
            </button>
          </div>
        </>
      )}
    </div>
  );
}