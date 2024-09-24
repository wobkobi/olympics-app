import React, { useState } from 'react';
import Image from 'next/image';
import Link from 'next/link';

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

interface TableProps {
  data: GroupedAthlete[];
  visibleColumns: string[];
  nocCountries: NocCountry[];
  onSort: (column: string) => void;
  sortColumn: string | null;
  sortDirection: 'asc' | 'desc';
  selectedOlympics?: string;
}

const Table: React.FC<TableProps> = ({
  data,
  visibleColumns,
  nocCountries,
  onSort,
  sortColumn,
  sortDirection,
}) => {
  const [expandedRow, setExpandedRow] = useState<number | null>(null);

  const toggleRow = (id: number) => {
    setExpandedRow((prev) => (prev === id ? null : id));
  };

  const getCountryName = (noc: string): string => {
    const country = nocCountries.find((c) => c.noc === noc);
    return country ? country.country : 'N/A';
  };

  return (
    <table className="w-full table-auto border-collapse">
      <thead>
        <tr>
          {visibleColumns.map((column) => (
            <th
              key={column}
              onClick={() => onSort(column)}
              className="cursor-pointer px-4 py-2 border-b border-gray-200"
            >
              {capitalizeFirstLetter(column)}
              {sortColumn === column && (
                <span>{sortDirection === 'asc' ? ' ▲' : ' ▼'}</span>
              )}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {data.map((athlete) => (
          <React.Fragment key={athlete.id}>
            <tr
              onClick={() => toggleRow(athlete.id)}
              className="cursor-pointer hover:bg-gray-100"
            >
              {visibleColumns.map((column) => {
                if (column === 'image') {
                  return (
                    <td key={column} className="px-4 py-2 border-b border-gray-200">
                      {athlete.image_url ? (
                        <Link href={`/athletes/${athlete.id}`}>
                          <Image
                            src={athlete.image_url}
                            alt={athlete.name}
                            width={100}
                            height={100}
                            className="object-cover rounded-full"
                          />
                        </Link>
                      ) : (
                        <div className="w-16 h-16 bg-gray-200 rounded-full flex items-center justify-center">
                          N/A
                        </div>
                      )}
                    </td>
                  );
                } else if (column === 'noc') {
                  return (
                    <td key={column} className="px-4 py-2 border-b border-gray-200">
                      {getCountryName(athlete.noc)}
                    </td>
                  );
                } else {
                  return (
                    <td key={column} className="px-4 py-2 border-b border-gray-200">
                      {athlete[column as keyof GroupedAthlete] as React.ReactNode}
                    </td>
                  );
                }
              })}
            </tr>
            {expandedRow === athlete.id && (
              <tr>
                <td colSpan={visibleColumns.length} className="px-4 py-2 border-b border-gray-200">
                  <div className="bg-gray-50 p-4 rounded">
                    <table className="w-full table-auto border-collapse">
                      <thead>
                        <tr>
                          <th className="px-4 py-2 border-b border-gray-200">Game</th>
                          <th className="px-4 py-2 border-b border-gray-200">Sport</th>
                          <th className="px-4 py-2 border-b border-gray-200">Event</th>
                          <th className="px-4 py-2 border-b border-gray-200">Team</th>
                          <th className="px-4 py-2 border-b border-gray-200">Position</th>
                        </tr>
                      </thead>
                      <tbody>
                        {athlete.events.map((event, index) => (
                          <tr key={index} className="hover:bg-gray-100">
                            <td className="px-4 py-2 border-b border-gray-200">{event.game}</td>
                            <td className="px-4 py-2 border-b border-gray-200">{event.sport}</td>
                            <td className="px-4 py-2 border-b border-gray-200">{event.event}</td>
                            <td className="px-4 py-2 border-b border-gray-200">{event.team}</td>
                            <td className="px-4 py-2 border-b border-gray-200">{event.position}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </td>
              </tr>
            )}
          </React.Fragment>
        ))}
      </tbody>
    </table>
  );
};

// Helper function to capitalize first letter
const capitalizeFirstLetter = (str: string) => {
  return str.charAt(0).toUpperCase() + str.slice(1);
};

export default Table;
