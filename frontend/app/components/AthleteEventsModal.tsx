// app/components/AthleteEventsModal.tsx

import React from "react";

interface AthleteEventsModalProps {
  isOpen: boolean;
  onClose: () => void;
  athleteName: string;
  events: { game: string; sport: string; event: string; team: string; position: string }[];
}

const AthleteEventsModal: React.FC<AthleteEventsModalProps> = ({
  isOpen,
  onClose,
  athleteName,
  events,
}) => {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black bg-opacity-50">
      <div className="bg-white rounded-lg w-3/4 max-w-3xl p-6">
        <div className="flex justify-between items-center mb-4">
          <h2 className="text-xl font-semibold">Events for {athleteName}</h2>
          <button onClick={onClose} className="text-red-500 font-bold text-xl">
            &times;
          </button>
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full bg-white">
            <thead>
              <tr className="w-full bg-gray-300 text-gray-800 uppercase text-sm leading-normal">
                <th className="py-2 px-4 text-left">Game</th>
                <th className="py-2 px-4 text-left">Sport</th>
                <th className="py-2 px-4 text-left">Event</th>
                <th className="py-2 px-4 text-left">Team</th>
                <th className="py-2 px-4 text-left">Position</th>
              </tr>
            </thead>
            <tbody className="text-gray-600 text-sm font-light">
              {events.length === 0 ? (
                <tr>
                  <td colSpan={5} className="py-2 px-4 text-center">
                    No events available.
                  </td>
                </tr>
              ) : (
                events.map((eventDetail, index) => (
                  <tr key={index} className="border-b border-gray-200 hover:bg-gray-100">
                    <td className="py-2 px-4 text-left">{eventDetail.game || "N/A"}</td>
                    <td className="py-2 px-4 text-left">{eventDetail.sport || "N/A"}</td>
                    <td className="py-2 px-4 text-left">{eventDetail.event || "N/A"}</td>
                    <td className="py-2 px-4 text-left">{eventDetail.team || "N/A"}</td>
                    <td className="py-2 px-4 text-left">{eventDetail.position || "N/A"}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

export default AthleteEventsModal;
